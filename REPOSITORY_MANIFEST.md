# REPOSITORY_MANIFEST — codex_qwen_GAIA

Agent 测量实验的工作负载执行框架。

- Repository: `codex_qwen_GAIA`
- Local path: `/home/codex_qwen_GAIA`
- Python: 3.14.4（`.venv` 不纳入版本管理）
- Codex CLI: 0.169.0

## 纳入版本管理的文件

| 文件 | 用途 | 依赖 |
|---|---|---|
| `run_gaia_codex.py` | GAIA workload adapter + 批次执行器（任务调度、bwrap 隔离、双端采集、七文件输出、答案校验） | `exa_tool_gateway.py`；`tshark`/`dumpcap`/`mitmdump`/`bwrap`/`codex`；`ssh spk` |
| `exa_tool_gateway.py` | Responses API 工具名网关（namespace -> `<ns>__<tool>`，响应再拆回 namespace/name） | Python stdlib only |
| `requirements.txt` | Agent 任务依赖锁定 | — |
| `README.md` | 仓库职责、环境、关键修复说明 | — |
| `EXPERIMENT_REPOSITORIES.md` | 三仓库职责、接口、部署顺序、版本基线 | — |
| `docs/workloads/GAIA.md` | GAIA workload 说明 | — |
| `docs/workloads/HumanEval.md` | HumanEval workload 说明（历史） | — |
| `docs/workloads/ADDING_A_BENCHMARK.md` | 新增 Benchmark adapter 的步骤 | — |

## 实验配置要点（当前默认值）

```text
Provider            Spark（127.0.0.1:18001 gateway -> 18000 SSH tunnel -> vLLM）
Context             32768（context-32k profile）
双端采集            local dumpcap + spark_capture.py worker
Prometheus / TCP    1.0 s
TLS Inspection      enabled（mitmdump --mode upstream:http://<wsl-gw>:7897）
Exa MCP             enabled，进程级 NO_PROXY 直连 mcp.exa.ai
单任务超时          由 --timeout 指定（全量实验使用 900 s）
结果结构            七文件
```

## 明确排除

```text
.venv/                      虚拟环境
DATASET/                    GAIA 数据集
results_codex_gaia/         实验产物（数 GB）
*.log, *_run.log            批次与单次运行日志
run_gaia_codex.py.pre-*.bak 历史脚本备份（保留在本地，不入库）
__pycache__/                Python 缓存
```

## 与其它仓库的接口

- 通过 `ssh spk` 调用服务端 `scripts/spark_capture.py`（worker / collect）
- 产出七文件；`server_traffic.pcapng`、`server_timeline.jsonl` 由测量框架解析
- 模型请求经 `exa_tool_gateway.py` 转发到 SSH 隧道
## 自包含测量副本（2026-11-26）

| 文件 | 用途 |
|---|---|
| spark_measurement.py | 随仓库携带的 SSH/双端采集协调副本 |
| scripts/experiment_config.py | 随仓库携带的 context/profile 配置 |
| scripts/telemetry_merge.py | 随仓库携带的 V3 telemetry 合并 |
| config/spark/config.toml、model_catalog.json | Provider 与模型 catalog 模板 |
| config/ssh_config.example | SSH 连接模板；真实 config/ssh_config 被 .gitignore 排除 |

codex_qwen 与 codex_qwen_GAIA 是两个独立 workload，均通过 ssh spk 调用服务端；
两者之间没有 Python import 依赖。exa_tool_gateway.py 仍只负责本仓库的模型请求。
