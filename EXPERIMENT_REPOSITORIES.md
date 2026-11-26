# EXPERIMENT_REPOSITORIES — 三个仓库的职责与部署顺序

本项目的三个仓库共同构成 **Agent Traffic Measurement** 的可复现实验体系。
它们是三个独立 Git 仓库，不是子模块关系。

## 职责

| 仓库 | 路径 | 职责 |
|---|---|---|
| `codex_qwen` | `/home/codex_qwen`（本机 WSL） | 流量采集、PCAPNG、HTTP/SSE、TCP、Prometheus、事件关联、解析与分析 |
| `codex_qwen_GAIA` | `/home/codex_qwen_GAIA`（本机 WSL） | 实验执行框架：Codex 启动、任务调度、实验配置、双端采集调用、结果记录、Benchmark 适配 |
| `waitung-inference-server-measurement` | `/home/huanglabspk/WaiTung`（DGX Spark） | 推理服务端测量：vLLM 插桩、EngineCore/Scheduler/API Server/SSE、Prometheus、TCP、GPU 与资源监控、模型安全启动与 watchdog |

## 逻辑依赖

```text
codex_qwen_GAIA
          |  启动 Agent、调度任务、发起采集
          v
codex_qwen
          |  解析 V3 telemetry / 服务端 PCAP
          v
waitung-inference-server-measurement
```

这不是严格的单向依赖：workload runner 同时通过 `ssh spk` 直接调用服务端
`scripts/spark_capture.py`，并把 `server_traffic.pcapng` / `server_timeline.jsonl`
交给测量框架解析。

## 真实接口

| 接口 | 说明 |
|---|---|
| `ssh spk` | runner -> 推理服务器；端口 18000 的 SSH 隧道承载模型 HTTP |
| `spark_capture.py worker --path ... --max-seconds ... --metrics-interval --tcp-interval --detailed-events` | runner 启动服务端采集 |
| `spark_capture.py collect --run-id --task-id` | 采集结束后回收服务端数据 |
| `agent-traffic-v3.0` | 服务端 telemetry JSONL 协议（`events-<pid>-NNN.jsonl`） |
| 七文件结果 | `task_prompt.txt`, `codex_output.json`, `codex_timeline.jsonl`, `result.json`, `raw_traffic.pcapng`, `server_traffic.pcapng`, `server_timeline.jsonl` |

## 部署顺序

1. **推理服务端**：`waitung-inference-server-measurement`
   - 部署 `scripts/` 与 `vllm_overlay/`
   - `scripts/start_vllm.sh --start --context-window 32768 --detailed-events 1`
   - 确认 `/health` 200、guard `armed`、watchdog 运行
2. **公共测量框架**：`codex_qwen`
   - 本机 WSL 检出，确认 `tshark`/`dumpcap` 可用
3. **工作负载执行框架**：`codex_qwen_GAIA`
   - 检出并安装 `requirements.txt`
   - `python run_gaia_codex.py --check-tools`
   - `python run_gaia_codex.py --all --list-tasks`（165 个 validation 任务）
   - 正式批次：`python -u run_gaia_codex.py --all --timeout 900`

## 版本基线

| 组件 | 版本 |
|---|---|
| vLLM | 0.24.0（5 个 overlay 文件 + `vllm-0.24.0-telemetry.patch`） |
| Codex CLI | 0.169.0 |
| Python（本机） | 3.14.4 |
| Python（服务端 vLLM venv） | 3.13 |
| 测量协议 | `agent-traffic-v3.0` |
| 模型 | Qwen3.6-35B-A3B，context 32768 |
| Exa MCP | `https://mcp.exa.ai/mcp?tools=web_search_exa`（streamable HTTP，匿名） |

每次重要实验需在实验记录中保存三个仓库的 commit SHA。
\n## 基线 commit\n\n\n\n三个仓库均为 GitHub Private Repository，默认分支 。\n

## 基线 commit

| 仓库 | 基线 commit |
|---|---|
| codex_qwen | `73589fb031e520f6ee41eaffb99baf747a0ca02d` |
| codex_qwen_GAIA | `270a442ae596900556109e0b951a021450ddb6b8` |
| waitung-inference-server-measurement | `bb929057f201be1a2d085350d02a18dc642de521` |

三个仓库均为 GitHub Private Repository，默认分支 `main`。
