use clap::{ArgAction, Parser, ValueEnum};
use freestiler_core::engine::{self, ProgressReporter, TileConfig};
use freestiler_core::mongo_writer::MongoConfig;
use freestiler_core::postgis::partition::PartitionConfig;
use freestiler_core::postgis_input::PostgisConfig;
use freestiler_core::sink::mongo::MongoSinkConfig;
use freestiler_core::{
    postgis_probe_and_maybe_load_layers_with_config, run_postgis_to_mongo_stream,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum MongoProfileArg {
    Recommended,
    Safe,
    #[value(alias = "high_detail")]
    HighDetail,
}

#[derive(Parser, Debug)]
#[command(name = "freestiler-postgis-mongo")]
#[command(about = "Stream PostGIS vector tiles into MongoDB using freestiler presets")]
struct Cli {
    /// PostGIS connection in ip:port:dbname:user:password or full postgresql:// URL form
    #[arg(long)]
    postgis: String,

    /// SQL query returning a PostGIS geometry column
    #[arg(long)]
    sql: String,

    /// MongoDB connection in host:port or full mongodb:// URI form
    #[arg(long = "mongo", alias = "mongo-uri")]
    mongo: String,

    /// MongoDB database name
    #[arg(long)]
    mongo_db: String,

    /// MongoDB collection name
    #[arg(long)]
    mongo_collection: String,

    /// Logical layer name used in tile metadata
    #[arg(long, default_value = "default")]
    layer_name: String,

    /// Mongo tuning preset: recommended=mvt z10..12, safe=mvt z6..12, high-detail=mvt z14..15
    #[arg(long, value_enum, default_value = "recommended")]
    mongo_profile: MongoProfileArg,

    /// Batch size for PostGIS cursor reads
    #[arg(long, default_value_t = 20_000)]
    batch_size: usize,

    /// Batch size for Mongo writes
    #[arg(long, default_value_t = 8_192)]
    mongo_batch_size: usize,

    /// Whether to use the streaming pipeline
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    streaming: bool,

    /// Replace existing documents with the same tile id
    #[arg(long, default_value_t = false)]
    upsert: bool,

    /// Create indexes on the target collection
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    create_indexes: bool,

    /// Optional geometry column name
    #[arg(long)]
    geom_column: Option<String>,

    /// Suppress progress output
    #[arg(long, default_value_t = false)]
    quiet: bool,
}

struct CliReporter {
    quiet: bool,
}

impl ProgressReporter for CliReporter {
    fn report(&self, msg: &str) {
        if !self.quiet {
            eprintln!("{}", msg);
        }
    }
}

fn parse_postgis_conn_str(input: &str) -> Result<String, String> {
    if input.contains("://") {
        return Ok(input.to_string());
    }

    let parts: Vec<&str> = input.splitn(5, ':').collect();
    if parts.len() != 5 || parts.iter().any(|part| part.is_empty()) {
        return Err(
            "Invalid --postgis value. Expected ip:port:dbname:user:password or postgresql://..."
                .to_string(),
        );
    }

    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        parts[3], parts[4], parts[0], parts[1], parts[2]
    ))
}

fn parse_mongo_uri(input: &str) -> Result<String, String> {
    if input.contains("://") {
        return Ok(input.to_string());
    }

    let parts: Vec<&str> = input.split(':').collect();
    if parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(
            "Invalid --mongo value. Expected host:port or mongodb://...".to_string(),
        );
    }

    Ok(format!("mongodb://{}", input))
}

fn apply_mongo_profile(profile: MongoProfileArg) -> TileConfig {
    match profile {
        MongoProfileArg::Recommended => TileConfig::mongo_recommended_default(),
        MongoProfileArg::Safe => TileConfig::mongo_safe_range(12),
        MongoProfileArg::HighDetail => TileConfig::mongo_high_detail_profile(),
    }
}

fn report_selected_profile(
    reporter: &dyn ProgressReporter,
    profile: MongoProfileArg,
    config: &TileConfig,
) {
    let profile_name = match profile {
        MongoProfileArg::Recommended => "recommended",
        MongoProfileArg::Safe => "safe",
        MongoProfileArg::HighDetail => "high_detail",
    };
    reporter.report(&format!(
        "Mongo profile: {} (zoom {}..{}, format {:?})",
        profile_name, config.min_zoom, config.max_zoom, config.tile_format
    ));
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let reporter = CliReporter { quiet: cli.quiet };

    let conn_str = parse_postgis_conn_str(&cli.postgis)?;
    let mongo_uri = parse_mongo_uri(&cli.mongo)?;
    let pg_config = PostgisConfig::new(&conn_str).batch_size(cli.batch_size);
    let config = apply_mongo_profile(cli.mongo_profile);

    reporter.report(&format!(
        "Connecting to PostGIS: {}",
        freestiler_core::tiler::mask_conn_str(&conn_str)
    ));
    report_selected_profile(&reporter, cli.mongo_profile, &config);
    engine::report_mongo_runtime_advisories(&config, &reporter);

    if cli.streaming {
        reporter.report("Path: streaming PostGIS -> Mongo");
        let mut sink_config =
            MongoSinkConfig::new(&mongo_uri, &cli.mongo_db, &cli.mongo_collection);
        sink_config.batch_size = cli.mongo_batch_size;
        sink_config.create_indexes = cli.create_indexes;
        sink_config.upsert = cli.upsert;

        let partition_config = PartitionConfig {
            partition_zoom: config.max_zoom,
            metatile_rows: 64,
        };

        let count = run_postgis_to_mongo_stream(
            &pg_config,
            &sink_config,
            &config,
            &partition_config,
            &cli.layer_name,
            &cli.sql,
            cli.geom_column.as_deref(),
            &reporter,
        )?;
        println!("{} tiles written to MongoDB", count);
        return Ok(());
    }

    reporter.report("Path: in-memory/by-zoom PostGIS -> Mongo");
    let mongo_config = MongoConfig::new(&mongo_uri, &cli.mongo_db, &cli.mongo_collection)
        .batch_size(cli.mongo_batch_size)
        .create_indexes(cli.create_indexes)
        .upsert(cli.upsert);

    let threshold = 200_000_u64;
    let (is_large, layers_opt) = postgis_probe_and_maybe_load_layers_with_config(
        &pg_config,
        &cli.sql,
        &cli.layer_name,
        config.min_zoom,
        config.max_zoom,
        cli.geom_column.as_deref(),
        threshold,
    )?;

    let count = if is_large || layers_opt.is_none() {
        engine::generate_postgis_query_to_mongo_by_zoom(
            &pg_config,
            &cli.sql,
            &cli.layer_name,
            cli.geom_column.as_deref(),
            &mongo_config,
            &config,
            &reporter,
        )?
    } else {
        let output = freestiler_core::OutputTarget::MongoDB {
            config: mongo_config,
        };
        engine::generate_tiles_to_target(
            &layers_opt.expect("small result should contain layers"),
            &output,
            &config,
            &reporter,
        )
        .map_err(|e| e.to_string())?
    };

    println!("{} tiles written to MongoDB", count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_mongo_profile, parse_mongo_uri, parse_postgis_conn_str, Cli, MongoProfileArg};
    use clap::Parser;
    use freestiler_core::engine::{
        MONGO_HIGH_DETAIL_MAX_ZOOM, MONGO_HIGH_DETAIL_MIN_ZOOM, MONGO_RECOMMENDED_MAX_ZOOM,
        MONGO_RECOMMENDED_MIN_ZOOM, MONGO_SAFE_MIN_ZOOM,
    };

    #[test]
    fn parses_short_postgis_connection_string() {
        let actual =
            parse_postgis_conn_str("10.1.0.16:5433:geoc_data:postgres:postgres").unwrap();
        assert_eq!(
            actual,
            "postgresql://postgres:postgres@10.1.0.16:5433/geoc_data"
        );
    }

    #[test]
    fn keeps_full_postgis_url_unchanged() {
        let actual =
            parse_postgis_conn_str("postgresql://postgres:postgres@10.1.0.16:5433/geoc_data")
                .unwrap();
        assert_eq!(
            actual,
            "postgresql://postgres:postgres@10.1.0.16:5433/geoc_data"
        );
    }

    #[test]
    fn parses_short_mongo_connection_string() {
        let actual = parse_mongo_uri("localhost:27017").unwrap();
        assert_eq!(actual, "mongodb://localhost:27017");
    }

    #[test]
    fn keeps_full_mongo_url_unchanged() {
        let actual = parse_mongo_uri("mongodb://localhost:27017").unwrap();
        assert_eq!(actual, "mongodb://localhost:27017");
    }

    #[test]
    fn applies_expected_mongo_profiles() {
        let recommended = apply_mongo_profile(MongoProfileArg::Recommended);
        assert_eq!(recommended.min_zoom, MONGO_RECOMMENDED_MIN_ZOOM);
        assert_eq!(recommended.max_zoom, MONGO_RECOMMENDED_MAX_ZOOM);

        let safe = apply_mongo_profile(MongoProfileArg::Safe);
        assert_eq!(safe.min_zoom, MONGO_SAFE_MIN_ZOOM);
        assert_eq!(safe.max_zoom, 12);

        let high_detail = apply_mongo_profile(MongoProfileArg::HighDetail);
        assert_eq!(high_detail.min_zoom, MONGO_HIGH_DETAIL_MIN_ZOOM);
        assert_eq!(high_detail.max_zoom, MONGO_HIGH_DETAIL_MAX_ZOOM);
    }

    #[test]
    fn accepts_high_detail_alias_with_underscore() {
        let cli = Cli::try_parse_from([
            "freestiler-postgis-mongo",
            "--postgis",
            "10.1.0.16:5433:geoc_data:postgres:postgres",
            "--sql",
            "SELECT 1",
            "--mongo",
            "localhost:27017",
            "--mongo-db",
            "tiles",
            "--mongo-collection",
            "cities",
            "--mongo-profile",
            "high_detail",
        ])
        .expect("parse cli");

        assert_eq!(cli.mongo_profile, MongoProfileArg::HighDetail);
    }
}
