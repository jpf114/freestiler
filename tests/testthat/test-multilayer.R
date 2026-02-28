test_that("freestile creates multi-layer MVT PMTiles", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(counties = nc, centroids = pts),
    output,
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile creates multi-layer MLT PMTiles", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(counties = nc, centroids = pts),
    output,
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile_layer sets per-layer zoom range", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(
      counties = freestile_layer(nc, min_zoom = 0, max_zoom = 6),
      centroids = freestile_layer(pts, min_zoom = 4, max_zoom = 10)
    ),
    output,
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 10,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_layer validates input", {
  expect_error(freestile_layer(data.frame(x = 1)), "must be an sf object")
})

test_that("multi-layer input requires named list", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  expect_error(
    freestile(list(nc, nc), output, quiet = TRUE),
    "named list"
  )
})

test_that("single sf input still works (backward compat)", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    nc,
    output,
    layer_name = "test",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 4,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
