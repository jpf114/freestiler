# 2026-04-26 Mongo Profile 压测结论

测试对象：
- 项目：`freestiler`
- 数据源：`PostGIS`
- 输出：`MongoDB`
- 表：`public.ht_tyg5c32ihg_sys_ht_mark`
- 模式：`streaming=true`

测试入口：
- [python/scripts/benchmark_mongo_profiles.py](/D:/Code/MyProject/freestiler/python/scripts/benchmark_mongo_profiles.py)

## 当前结论

| profile | zoom | 耗时 | tile 数 | 数据体积 size | 最大单 tile data |
| --- | --- | ---: | ---: | ---: | ---: |
| `recommended` | `10..12` | `287.51s` | `20395` | `75,403,844` bytes | `32,786` bytes |
| `safe` | `6..12` | `532.44s` | `21569` | `166,413,139` bytes | `4,708,331` bytes |
| `high_detail` | `14..15` | `1167.16s` | `958771` | `203,099,828` bytes | `314` bytes |

## 现状判断

`recommended`
- 当前最均衡。
- 单 tile 体积很小，离 Mongo `16MB` 上限很远。
- 适合作为默认生产 profile。

`safe`
- 可以稳定完成。
- 比 `recommended` 更重，但能够覆盖更低层级。
- 更适合“必须覆盖更低 zoom”的场景，不适合作为默认值。

`high_detail`
- 可以稳定完成。
- 主要成本来自 tile 数量、编码和 Mongo 写入。
- 适合作为按需高精度模式，不适合作为默认在线全量任务。

## 结论

- 当前默认推荐 profile 仍然应为 `mongo_profile="recommended"`。
- 如果业务必须覆盖更低层级，可使用 `mongo_profile="safe"`。
- 如果业务需要更高精度，可使用 `mongo_profile="high_detail"`，但应视为高成本模式。
- 对这张真实大表，Mongo 输出的安全边界仍然成立：
  - `min_zoom >= 6`
  - `10..12` 是最稳妥的默认区间
  - `14..15` 适合作为按需高精度区间
  - `z5` 及以下不建议进入 Mongo 文档方案

补充：
- 当前默认吞吐参数已经单独调优完成，见
  [2026-04-26-批量参数压测结论.md](/D:/Code/MyProject/freestiler/reports/2026-04-26-批量参数压测结论.md)。
