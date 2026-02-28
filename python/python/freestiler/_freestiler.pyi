"""Type stubs for the freestiler Rust extension module."""

def _freestile(
    layers: list[dict],
    output_path: str,
    tile_format: str,
    min_zoom: int,
    max_zoom: int,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
    drop_rate: float,
    cluster_distance: float,
    cluster_maxzoom: int,
    do_coalesce: bool,
) -> str: ...
