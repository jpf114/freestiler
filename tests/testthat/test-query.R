# Helper to check if duckdb feature is compiled
.has_duckdb <- function() {
  result <- rust_freestile_duckdb_query("", "", "", "", "mvt", 0L, 6L, -1L,
    TRUE, -1.0, -1.0, -1L, FALSE, TRUE)
  !startsWith(result, "Error: DuckDB support not compiled")
}

test_that("freestile_query creates PMTiles from SQL query", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not(.has_duckdb(), message = "DuckDB feature not compiled")

  nc_path <- system.file("shape/nc.shp", package = "sf")

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
    layer_name = "counties",
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile_query works with MVT format", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not(.has_duckdb(), message = "DuckDB feature not compiled")

  nc_path <- system.file("shape/nc.shp", package = "sf")

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_query returns error without duckdb feature", {
  skip_on_cran()
  skip_if(.has_duckdb(),
    message = "DuckDB feature IS compiled, skip negative test")

  expect_error(
    freestile_query("SELECT 1", tempfile(fileext = ".pmtiles"), quiet = TRUE),
    "not compiled"
  )
})
