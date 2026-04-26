# 2026-04-26 Mongo Profile 压测结论

测试对象：
- 项目：`freestiler`
- 数据源：`PostGIS`
- 输出：`MongoDB`
- 表：`public.ht_tyg5c32ihg_sys_ht_mark`
- 模式：`streaming=true`
- 参数：`batch_size=10000`、`mongo_batch_size=4096`、`create_indexes=true`

说明：
- 这份报告反映的是当时 profile 横向对比所使用的批量参数基线。
- 后续已经单独完成批量参数调优，新的默认吞吐参数见
  [reports/2026-04-26-批量参数压测结论.md](/D:/Code/MyProject/freestiler/reports/2026-04-26-批量参数压测结论.md)。

测试命令基于：
- [python/scripts/benchmark_mongo_profiles.py](/D:/Code/MyProject/freestiler/python/scripts/benchmark_mongo_profiles.py)

原始结果：
- [reports/2026-04-26-mongo-profile-benchmark.json](/D:/Code/MyProject/freestiler/reports/2026-04-26-mongo-profile-benchmark.json)
- [reports/2026-04-26-mongo-profile-high-detail.json](/D:/Code/MyProject/freestiler/reports/2026-04-26-mongo-profile-high-detail.json)

## 结果汇总

| profile | zoom | 耗时 | tile 数 | 数据体积 size | 最大单 tile data |
| --- | --- | ---: | ---: | ---: | ---: |
| `recommended` | `10..12` | `287.51s` | `20395` | `75,403,844` bytes | `32,786` bytes |
| `safe` | `6..12` | `532.44s` | `21569` | `166,413,139` bytes | `4,708,331` bytes |
| `high_detail` | `14..15` | `1167.16s` | `958771` | `203,099,828` bytes | `314` bytes |

## 逐项判断

`recommended`
- 当前最均衡。
- 耗时约 `4.8` 分钟。
- 最大单 tile 很小，离 Mongo `16MB` 上限非常远。
- 适合作为默认生产参数。

`safe`
- 可以稳定完成。
- 耗时约 `8.9` 分钟，比 `recommended` 慢约 `1.85x`。
- `z6` 最大 tile 约 `4.7MB`，仍在安全范围内，但已经明显进入“大 tile 区域”。
- 更适合“必须覆盖更低层级”的保守方案，不适合默认值。

`high_detail`
- 可以稳定完成。
- 耗时约 `19.5` 分钟。
- tile 数暴涨到 `958771`，主要成本来自 tile 数量、编码和 Mongo 写入，不再是单 tile 过大问题。
- 更适合作为按需开启的高精度方案，不适合默认在线全量任务。

## 结论

- 默认推荐参数仍然应为 `mongo_profile="recommended"`。
- 如果业务必须覆盖更低层级，可使用 `mongo_profile="safe"`，但要接受明显更高的写入成本。
- `mongo_profile="high_detail"` 在 `14..15` 是可用的，但应视为高成本模式。
- 当前真实大表下，风险边界依然成立：
  - `min_zoom >= 6` 才适合 Mongo 输出
  - `10..12` 仍然是最稳妥的默认区间
  - `14..15` 可作为按需高精度区间
  - `z5` 及以下仍不建议进入 Mongo 文档方案

## 下一步建议

- 把 `recommended` 明确固定为默认生产参数。
- 把这份 profile 压测脚本接入标准回归流程。
- 再补一轮 `batch_size` / `mongo_batch_size` 组合压测，确定最终默认吞吐参数。
