---
name: freestiler-postgis-mongo-refactor
description: Freestiler切片工具PostGIS输入MongoDB输出改造方案
type: project
---

# Freestiler切片工具改造方案

## 背景

将 freestiler 切片工具从当前的输入输出方式改造为：
- **输入**：PostGIS 数据库
- **输出**：MongoDB 数据库（存储 x、y、z、d）

## 一、当前架构分析

### 1.1 项目结构

```
freestilerpro/
├── src/rust/
│   ├── freestiler-core/          # Rust核心引擎
│   │   ├── src/
│   │   │   ├── lib.rs            # 模块入口
│   │   │   ├── tiler.rs          # 核心切片逻辑、Feature/Geometry数据结构
│   │   │   ├── engine.rs         # 切片生成流程、TileConfig配置
│   │   │   ├── file_input.rs     # 文件输入（GeoParquet、DuckDB）
│   │   │   ├── streaming.rs      # 流式处理（仅支持DuckDB）
│   │   │   ├── pmtiles_writer.rs # PMTiles输出
│   │   │   ├── clip.rs           # 几何裁剪
│   │   │   ├── mlt.rs            # MLT格式编码
│   │   │   ├── mvt.rs            # MVT格式编码
│   │   │   └── ...
│   │   └── Cargo.toml
│   └── src/lib.rs                # R绑定
├── python/
│   ├── src/lib.rs                # Python绑定
│   └── python/freestiler/        # Python包
└── R/                            # R包接口
```

### 1.2 当前数据流

```
输入源 → LayerData → 切片引擎 → PMTiles文件
```

**当前支持的输入源**：
- R/Python 内存中的 sf/sfg 对象
- GeoParquet 文件（通过 `geoparquet` feature）
- DuckDB SQL 查询（通过 `duckdb` feature）
- DuckDB 文件（GeoJSON、Shapefile等）

**当前支持的输出**：
- PMTiles 文件（MVT 或 MLT 格式）

### 1.3 核心数据结构

```rust
// tiler.rs - Feature结构
pub struct Feature {
    pub id: Option<u64>,
    pub geometry: Geometry,          // Point/LineString/Polygon等
    pub properties: Vec<PropertyValue>,
}

pub struct LayerData {
    pub name: String,
    pub features: Vec<Feature>,
    pub prop_names: Vec<String>,
    pub prop_types: Vec<String>,
    pub min_zoom: u8,
    pub max_zoom: u8,
}

pub struct TileCoord {
    pub z: u8,
    pub x: u32,
    pub y: u32,
}
```

### 1.4 切片生成流程 (engine.rs)

```
1. 接收LayerData
2. 按zoom层级遍历
3. 特征分配到切片（assign_features_to_tiles）
4. 几何裁剪（clip）
5. 简化处理（simplify）
6. 编码为MVT/MLT字节
7. 写入PMTiles文件
```

---

## 二、改造工作分解

### 2.1 PostGIS 输入改造

**目标**：新增 PostGIS 数据库连接和查询功能

#### 2.1.1 Rust 侧改造

**新增模块**: `src/rust/freestiler-core/src/postgis_input.rs`

```rust
// 需要添加的依赖
[dependencies]
postgres = "0.19"           # Postgres客户端
postgis = "0.9"             # PostGIS类型支持（可选，或使用WKB解析）
```

**功能实现**：

1. **数据库连接管理**
   - `PostgisConnection` 结构体封装连接池
   - 支持环境变量或配置参数传递连接信息
   - 连接字符串格式：`postgresql://user:pass@host:port/dbname`

2. **空间查询接口**
   - `postgis_query_to_layers()` 函数
   - 自动检测 SRID 并转换到 WGS84（EPSG:4326）
   - 支持 PostGIS 函数：`ST_AsWKB()`, `ST_Transform()`, `ST_SRID()`

3. **流式读取**
   - 参考 `streaming.rs` 模式
   - 分批读取大表数据避免内存溢出
   - 使用 PostgreSQL cursor 实现分页

**参考 DuckDB 实现**（file_input.rs:344-482）：

```rust
pub fn postgis_query_to_layers(
    conn_str: &str,              // PostgreSQL连接字符串
    sql: &str,                   // SQL查询（需返回geometry列）
    layer_name: &str,
    min_zoom: u8,
    max_zoom: u8,
) -> Result<Vec<LayerData>, String> {
    // 1. 建立连接
    // 2. 执行 DESCRIBE 查询获取列信息
    // 3. 检测 geometry 列和 SRID
    // 4. 构建包含 ST_AsWKB 的查询
    // 5. 解析结果到 Feature
    // 6. 返回 LayerData
}
```

#### 2.1.2 Python/R 绑定扩展

**Python**: `python/src/lib.rs` 新增函数

```rust
#[pyfunction]
fn _freestile_postgis(
    conn_str: &str,
    sql: &str,
    output_path: &str,
    layer_name: &str,
    // ... 其他参数同现有接口
) -> PyResult<String>
```

**R**: `src/rust/src/lib.rs` 新增函数

```rust
#[extendr]
fn rust_freestile_postgis(...)
```

---

### 2.2 MongoDB 输出改造

**目标**：新增 MongoDB 输出，存储切片数据为 `{x, y, z, d}` 文档

#### 2.2.1 MongoDB 数据模型

```javascript
// MongoDB文档结构
{
    "_id": ObjectId,
    "x": NumberInt,          // 切片X坐标
    "y": NumberInt,          // 切片Y坐标
    "z": NumberInt,          // 切片层级
    "d": BinData,            // 切片数据（gzip压缩的MVT/MLT）
}
```

**索引设计**：
```javascript
// 复合索引支持快速切片查询
db.tiles.createIndex({ "z": 1, "x": 1, "y": 1 })
```

#### 2.2.2 Rust 侧改造

**新增模块**: `src/rust/freestiler-core/src/mongo_writer.rs`

```rust
// 需要添加的依赖
[dependencies]
mongodb = "2.8"              # MongoDB官方驱动
bson = "2.10"                # BSON序列化
tokio = { version = "1", features = ["full"] }  # 异步运行时
```

**功能实现**：

```rust
pub struct MongoTileWriter {
    client: mongodb::Client,
    db: mongodb::Database,
    collection: mongodb::Collection<Document>,
}

impl MongoTileWriter {
    pub async fn new(conn_str: &str, db_name: &str, coll_name: &str) -> Result<Self, String>
    
    pub async fn write_tiles(
        &self,
        tiles: Vec<(TileCoord, Vec<u8>)>,
        layer_name: &str,
        tile_format: TileFormat,
    ) -> Result<u64, String> {
        // 1. 批量构建文档
        // 2. 使用 bulk_write 批量插入
        // 3. 返回插入数量
    }
}

// 同步包装器（兼容现有同步架构）
pub fn write_tiles_sync(
    mongo_uri: &str,
    tiles: Vec<(TileCoord, Vec<u8>)>,
    ...
) -> Result<(), String>
```

#### 2.2.3 输出格式选择

需要支持两种输出模式：
1. **PMTiles文件** - 保持现有功能
2. **MongoDB** - 新增功能

**配置扩展**：

```rust
pub enum OutputTarget {
    Pmtiles(String),              // 文件路径
    MongoDb {
        uri: String,              // MongoDB连接字符串
        database: String,         // 数据库名
        collection: String,       // 集合名
    },
}

pub struct TileConfig {
    pub tile_format: TileFormat,
    pub output: OutputTarget,     // 新增
    // ... 其他配置
}
```

---

### 2.3 新增完整工作流

**新增函数**: `_freestile_postgis_to_mongo`

```rust
pub fn generate_tiles_to_mongo(
    layers: &[LayerData],
    mongo_config: &MongoConfig,
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<u64, String> {
    // 1. 生成切片（复用现有引擎）
    let tiles = generate_tiles(layers, config, reporter)?;
    
    // 2. 写入MongoDB
    mongo_writer::write_tiles_sync(mongo_config, tiles, ...)?;
    
    Ok(tiles.len() as u64)
}
```

---

## 三、技术依赖

### 3.1 需添加的 Rust 依赖

```toml
[dependencies]
# PostGIS输入
postgres = "0.19"
# 可选：使用geo-types兼容的类型转换
# 或直接解析WKB（已有geozero依赖）

# MongoDB输出
mongodb = "2.8"
bson = "2.10"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### 3.2 特性开关设计

```toml
[features]
default = []
postgis = ["dep:postgres"]
mongodb = ["dep:mongodb", "dep:bson", "dep:tokio"]
```

---

## 四、改造工作量估算

| 模块 | 工作内容 | 估算复杂度 |
|------|----------|-----------|
| `postgis_input.rs` | PostGIS连接、查询、WKB解析 | 中等 |
| `mongo_writer.rs` | MongoDB连接、文档构建、批量写入 | 中等 |
| `engine.rs` | 输出目标选择逻辑 | 低 |
| Python绑定 | 新增PostGIS和Mongo接口 | 低 |
| R绑定 | 新增PostGIS和Mongo接口 | 低 |
| 测试 | PostGIS/MongoDB集成测试 | 中等 |
| 文档 | API文档、使用说明 | 低 |

**总体估算**: 约 2-3 周工作量（单人）

---

## 五、风险与挑战

### 5.1 PostGIS连接挑战
- 需处理多种 PostGIS 版本的兼容性
- SRID 检测和自动转换需测试
- 大数据量的流式读取需要 cursor 支持

### 5.2 MongoDB输出挑战
- MongoDB驱动是异步API，需与现有同步架构适配
- 批量写入性能优化（bulk_write）
- 切片数据的压缩策略

### 5.3 测试环境需求
- 需要 PostGIS 测试数据库
- 需要 MongoDB 测试实例
- CI/CD 环境需要测试容器支持

---

## 六、实施步骤建议

### Phase 1: PostGIS输入（优先）
1. 创建 `postgis_input.rs` 模块骨架
2. 实现基础连接和单表查询
3. 添加 SRID 检测和 WGS84 转换
4. Python/R 绑定测试
5. 流式读取优化（可选后续）

### Phase 2: MongoDB输出
1. 创建 `mongo_writer.rs` 模块骨架
2. 实现基础文档写入
3. 批量写入优化
4. 索引建议生成
5. 完整工作流测试

### Phase 3: 整合与文档
1. 输出目标选择配置
2. 完整的 PostGIS → MongoDB 工作流
3. 测试覆盖
4. API 文档更新
5. 使用示例编写

---

## 七、计划补充（基于代码深入分析）

### 7.1 架构发现与建议

#### 7.1.1 现有输入模块模式分析

通过分析 `file_input.rs` 发现：
- **WKB解析函数已存在**: `wkb_to_geometry()` 函数可以被 PostGIS 输入直接复用
- **DuckDB 模块提供了良好的参考模式**：包括列类型发现、SRID 检测、自动坐标转换
- **流式处理已有实现**: `streaming.rs` 提供了流式处理的完整框架，PostGIS 可以参考此模式

**建议优化**:
```rust
// PostGIS 输入应复用 file_input.rs 中的 wkb_to_geometry
// 而不是重新实现 WKB 解析
pub fn postgis_query_to_layers(...) -> Result<Vec<LayerData>, String> {
    // 复用现有 wkb_to_geometry 函数
    // 参考 duckdb_impl 模块的结构
}
```

#### 7.1.2 输出目标架构改进

当前 `engine.rs` 中的 `TileConfig` 不包含输出目标配置。建议引入更灵活的输出架构：

```rust
// 新增 OutputTarget 枚举
pub enum OutputTarget {
    Pmtiles { path: String },
    MongoDB { 
        uri: String,
        database: String,
        collection: String,
    },
    // 未来可扩展: S3, HTTP API 等
}

// 修改 TileConfig
pub struct TileConfig {
    pub tile_format: TileFormat,
    pub output: OutputTarget,  // 新增
    pub min_zoom: u8,
    pub max_zoom: u8,
    // ... 其他字段保持不变
}

// 新增统一的生成函数
pub fn generate_tiles_to_target(
    layers: &[LayerData],
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<u64, String> {
    let tiles = generate_tiles(layers, config, reporter)?;
    
    match &config.output {
        OutputTarget::Pmtiles { path } => {
            pmtiles_writer::write_pmtiles(...)
        },
        OutputTarget::MongoDB { uri, database, collection } => {
            mongo_writer::write_tiles_sync(uri, database, collection, tiles, ...)
        },
    }
}
```

### 7.2 PostGIS 输入详细实现补充

#### 7.2.1 Rust 依赖选择建议

**推荐使用 `tokio-postgres` + `postgis` 组合**：

```toml
[dependencies]
# 异步 Postgres 客户端（更现代、性能更好）
tokio-postgres = { version = "0.7", features = ["runtime"] }

# 或使用同步客户端（更简单，与现有架构兼容）
postgres = "0.19"

# PostGIS 类型支持（可选）
postgis = "0.9"

# 已有依赖可复用
geozero = { version = "0.13", features = ["with-wkb", "with-geo"] }
```

**决策建议**: 
- 如果追求简单和与现有同步架构兼容，使用 `postgres` 同步客户端
- 如果需要高性能流式读取，使用 `tokio-postgres` 异步客户端

#### 7.2.2 PostGIS 查询 SQL 构建策略

参考 `file_input.rs:344-482` 中的 DuckDB 模式：

```rust
// 1. 使用 DESCRIBE 或系统表获取列信息
let discover_sql = format!(
    "SELECT column_name, data_type 
     FROM information_schema.columns 
     WHERE table_name = '{}'",
    table_name
);

// 2. 检测 geometry 列和 SRID
let srid_sql = format!(
    "SELECT ST_SRID(geom) FROM {} WHERE geom IS NOT NULL LIMIT 1",
    geom_col_name
);

// 3. 构建包含 ST_AsWKB 的查询
let wkb_sql = if source_srid == 4326 {
    format!("SELECT *, ST_AsWKB(\"{}\") AS __wkb FROM {}", geom_col_name, table_name)
} else {
    format!(
        "SELECT *, ST_AsWKB(ST_Transform(\"{}\", 4326)) AS __wkb FROM {}",
        geom_col_name, table_name
    )
};
```

#### 7.2.3 流式读取策略

参考 `streaming.rs` 的模式，PostGIS 流式读取应使用 **Cursor**：

```rust
// PostgreSQL cursor 分页读取
pub fn postgis_streaming_query(
    conn: &postgres::Client,
    sql: &str,
    batch_size: usize,  // 例如 10000
) -> Result<Vec<Feature>, String> {
    // 1. 创建 cursor
    conn.execute("BEGIN", &[])?;
    conn.execute(
        &format!("DECLARE __cursor CURSOR FOR {}", sql),
        &[]
    )?;
    
    // 2. 分批 FETCH
    loop {
        let batch_sql = format!("FETCH {} FROM __cursor", batch_size);
        let rows = conn.query(&batch_sql, &[])?;
        if rows.is_empty() {
            break;
        }
        // 处理 batch...
    }
    
    // 3. 关闭 cursor
    conn.execute("CLOSE __cursor", &[])?;
    conn.execute("COMMIT", &[])?;
}
```

### 7.3 MongoDB 输出详细实现补充

#### 7.3.1 异步适配策略

MongoDB 驱动是异步的，而现有架构是同步的。有三种适配方案：

**方案 A: 同步包装器（推荐）**

```rust
// 使用 tokio runtime 包装异步 MongoDB 操作
pub fn write_tiles_sync(
    mongo_uri: &str,
    database: &str,
    collection: &str,
    tiles: Vec<(TileCoord, Vec<u8>)>,
) -> Result<u64, String> {
    // 创建 tokio runtime（或使用全局 runtime）
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("Cannot create tokio runtime: {}", e))?;
    
    rt.block_on(async {
        write_tiles_async(mongo_uri, database, collection, tiles).await
    })
}

async fn write_tiles_async(...) -> Result<u64, String> {
    let client = mongodb::Client::connect(mongo_uri).await?;
    let coll = client.database(database).collection(collection);
    
    // 构建文档并批量插入
    let docs: Vec<Document> = tiles.iter().map(|(coord, data)| {
        doc! {
            "z": coord.z as i32,
            "x": coord.x as i32,
            "y": coord.y as i32,
            "d": Binary { subtype: BinarySubtype::Generic, bytes: data.clone() }
        }
    }).collect();
    
    coll.insert_many(docs).await?;
    Ok(tiles.len() as u64)
}
```

**方案 B: 全局异步 runtime**

如果性能关键，可以在应用启动时创建全局 tokio runtime：

```rust
// 在 lib.rs 中
lazy_static::lazy_static! {
    static ref TOKIO_RT: tokio::runtime::Runtime = 
        tokio::runtime::Runtime::new().unwrap();
}
```

**方案 C: 完全异步重构（大工作量）**

将整个切片生成流程改为异步，需要重构 `engine.rs`。

#### 7.3.2 批量写入优化

MongoDB bulk write 性能优化建议：

```rust
// 1. 分批写入（避免单次操作过大）
const BATCH_SIZE: usize = 1000;

for batch in tiles.chunks(BATCH_SIZE) {
    let docs = build_documents(batch);
    coll.insert_many(docs).await?;
}

// 2. 使用 ordered=false 提高并发写入
let options = InsertManyOptions {
    ordered: false,  // 继续写入即使部分失败
    ..Default::default()
};
coll.insert_many_with_options(docs, options).await?;

// 3. 预压缩切片数据（减少网络传输）
let compressed = gzip_compress(&tile_bytes)?;
```

#### 7.3.3 MongoDB 索引自动创建

```rust
pub async fn ensure_indexes(coll: &Collection<Document>) -> Result<(), String> {
    // 创建复合索引
    let index = IndexModel::builder()
        .keys(doc! { "z": 1, "x": 1, "y": 1 })
        .options(IndexOptions::builder()
            .unique(true)  // 确保切片唯一性
            .name("tile_coords_idx")
            .build())
        .build();
    
    coll.create_index(index).await?;
    Ok(())
}
```

### 7.4 Python 绑定扩展补充

#### 7.4.1 新增函数签名

```rust
#[pyfunction]
#[pyo3(signature = (conn_str, sql, output_config, layer_name, tile_format, 
    min_zoom, max_zoom, base_zoom, do_simplify, quiet, drop_rate, 
    cluster_distance, cluster_maxzoom, do_coalesce))]
fn _freestile_postgis(
    conn_str: &str,
    sql: &str,
    output_config: &str,  // "pmtiles:/path/to/file" 或 "mongo://uri/db/coll"
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> PyResult<String>
```

#### 7.4.2 输出配置解析

```rust
fn parse_output_config(config: &str) -> Result<OutputTarget, String> {
    if config.starts_with("pmtiles:") {
        Ok(OutputTarget::Pmtiles {
            path: config.strip_prefix("pmtiles:").unwrap().to_string()
        })
    } else if config.starts_with("mongo:") {
        // 格式: mongo://uri/database/collection
        let parts = config.strip_prefix("mongo:").unwrap()
            .split('/').collect::<Vec<_>>();
        Ok(OutputTarget::MongoDB {
            uri: parts[0].to_string(),
            database: parts[1].to_string(),
            collection: parts[2].to_string(),
        })
    } else {
        Err(format!("Unknown output config: {}", config))
    }
}
```

### 7.5 特性开关更新

```toml
[features]
default = []
geoparquet = ["dep:parquet", "dep:arrow-array", "dep:arrow-schema", "dep:geozero"]
duckdb = ["dep:duckdb", "dep:geozero"]
postgis = ["dep:postgres", "dep:geozero"]  # 复用 geozero 的 WKB 解析
mongodb = ["dep:mongodb", "dep:bson", "dep:tokio"]
fastpfor = ["dep:fastpfor"]
fsst = ["dep:fsst-rs"]
```

### 7.6 测试策略补充

#### 7.6.1 PostGIS 测试

```rust
#[cfg(feature = "postgis")]
#[cfg(test)]
mod postgis_tests {
    use super::*;
    
    // 需要 Docker 或本地 PostGIS 实例
    // CI: 使用 testcontainers
    
    #[test]
    fn test_postgis_connection() {
        let conn_str = "postgresql://test:test@localhost:5432/test";
        let sql = "SELECT geom FROM test_points LIMIT 10";
        let layers = postgis_query_to_layers(conn_str, sql, "test", 0, 14).unwrap();
        assert!(!layers.is_empty());
    }
    
    #[test]
    fn test_srid_detection() {
        // 测试 EPSG:3857 自动转换为 EPSG:4326
    }
}
```

#### 7.6.2 MongoDB 测试

```rust
#[cfg(feature = "mongodb")]
#[cfg(test)]
mod mongo_tests {
    use super::*;
    
    #[test]
    fn test_mongo_write() {
        let tiles = vec![
            (TileCoord { z: 0, x: 0, y: 0 }, vec![1, 2, 3]),
        ];
        write_tiles_sync("mongodb://localhost:27017", "test", "tiles", tiles).unwrap();
        
        // 验证写入
    }
}
```

### 7.7 完整工作流示例

```python
# Python 使用示例
import freestiler

# PostGIS → MongoDB 工作流
freestiler._freestile_postgis(
    conn_str="postgresql://user:pass@host:5432/gis_db",
    sql="SELECT geom, name, population FROM cities WHERE country = 'CN'",
    output_config="mongo://mongodb://localhost:27017/tiles_db/china_cities",
    layer_name="cities",
    tile_format="mvt",
    min_zoom=4,
    max_zoom=14,
    ...
)

# PostGIS → PMTiles 工作流（保持兼容）
freestiler._freestile_postgis(
    conn_str="postgresql://...",
    sql="SELECT ...",
    output_config="pmtiles:/output/cities.pmtiles",
    ...
)
```

### 7.8 性能考量补充

#### 7.8.1 PostGIS 输入性能

- **连接池**: 使用 `r2d2-postgres` 或 `deadpool-postgres` 管理连接池
- **流式读取**: 对于大数据集，使用 cursor 分批读取
- **索引依赖**: 确保 PostGIS 表有空间索引（GiST）

#### 7.8.2 MongoDB 输出性能

- **批量大小**: 推荐 500-1000 个切片/批次
- **压缩**: 预压缩切片数据可减少 50-70% 网络传输
- **连接池**: MongoDB 驱动内置连接池，无需额外配置

---

## 八、结论

改造 freestiler 以支持 PostGIS 输入和 MongoDB 输出是可行的，主要工作集中在：

1. **新增两个 Rust 模块**：`postgis_input.rs` 和 `mongo_writer.rs`
2. **扩展现有绑定**：Python/R 接口新增函数
3. **调整输出配置**：支持多种输出目标
4. **复用现有基础设施**：WKB 解析、LayerData 结构、切片生成引擎

现有架构设计良好，`LayerData` 和切片生成逻辑可以复用，只需替换输入源和输出目标即可实现改造。

**Why:** 用户需要从PostGIS数据库读取空间数据切片后存储到MongoDB，而不是现有的文件输入和PMTiles输出方式。

**How to apply:** 按照上述Phase分阶段实施，先完成PostGIS输入，再实现MongoDB输出，最后整合完整工作流。

---

## 九、实施时间估算（更新）

| 模块 | 工作内容 | 复杂度 | 估算天数 |
|------|----------|--------|---------|
| `postgis_input.rs` | 连接管理、查询、WKB解析、流式读取 | 中等 | 3-5 天 |
| `mongo_writer.rs` | MongoDB 连接、文档构建、批量写入、异步适配 | 中等 | 3-4 天 |
| `engine.rs` | OutputTarget 架构、generate_tiles_to_target | 低 | 1-2 天 |
| Python 绑定 | _freestile_postgis 函数 | 低 | 1-2 天 |
| R 绑定 | rust_freestile_postgis 函数 | 低 | 1 天 |
| 测试 | PostGIS/MongoDB 集成测试 | 中等 | 2-3 天 |
| 文档 | API 文档、使用示例 | 低 | 1 天 |

**总计**: 约 12-18 天（单人）

---

## 十、优先级排序

1. **Phase 1: PostGIS 输入（核心）**
   - 创建 `postgis_input.rs`
   - 实现基础连接和查询
   - 复用 `wkb_to_geometry` 
   - SRID 检测和转换
   - Python 绑定测试

2. **Phase 2: MongoDB 输出（核心）**
   - 创建 `mongo_writer.rs`
   - 异步适配（同步包装器）
   - 批量写入优化
   - 索引自动创建

3. **Phase 3: 输出架构重构**
   - OutputTarget 枚举
   - generate_tiles_to_target
   - 配置解析逻辑

4. **Phase 4: 整合与优化**
   - 完整工作流测试
   - 流式读取优化（可选）
   - 性能测试和调优

5. **Phase 5: 文档与发布**
   - API 文档
   - 使用示例
   - README 更新