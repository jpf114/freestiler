test_that("freestile with drop_rate produces output", {
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
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 8,
    drop_rate = 2.5,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("drop_rate produces smaller output than no dropping", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output_nodrop <- tempfile(fileext = ".pmtiles")
  output_drop <- tempfile(fileext = ".pmtiles")
  on.exit({
    unlink(output_nodrop)
    unlink(output_drop)
  }, add = TRUE)

  freestile(nc, output_nodrop, layer_name = "nc", tile_format = "mvt",
    min_zoom = 0, max_zoom = 8, quiet = TRUE)
  freestile(nc, output_drop, layer_name = "nc", tile_format = "mvt",
    min_zoom = 0, max_zoom = 8, drop_rate = 2.5, quiet = TRUE)

  size_nodrop <- file.info(output_nodrop)$size
  size_drop <- file.info(output_drop)$size

  # Dropping should produce a smaller or equal file
  expect_true(size_drop <= size_nodrop)
})

test_that("drop_rate works with point data", {
  skip_if_not_installed("sf")

  pts <- sf::st_as_sf(
    data.frame(
      name = paste0("P", 1:50),
      lon = runif(50, -80, -75),
      lat = runif(50, 35, 40)
    ),
    coords = c("lon", "lat"),
    crs = 4326
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    pts,
    output,
    layer_name = "points",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 10,
    drop_rate = 2.0,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
