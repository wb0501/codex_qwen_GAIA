# EXPERIMENT_REPOSITORIES — Agent Traffic Measurement 代码仓库

本项目的代码按职责拆分为 5 个独立 GitHub Private Repository。Codex 侧三个
仓库构成测量主链；Claude 侧两个仓库是并列的 Agent runner，用于跨模型/跨
Benchmark 对照。

## 仓库职责

| 仓库 | 路径 | 职责 |
|---|---|---|
| `codex_qwen` | `/home/codex_qwen`（本机 WSL） | 公共测量框架：PCAPNG、HTTP/SSE、TCP、Prometheus、事件关联、解析与分析 |
| `codex_qwen_GAIA` | `/home/codex_qwen_GAIA`（本机 WSL） | Codex 工作负载执行框架：任务调度、Bubblewrap、双端采集调用、七文件结果、GAIA/HumanEval adapter |
| `waitung-inference-server-measurement` | `/home/huanglabspk/WaiTung`（DGX Spark） | 推理服务端测量：vLLM 0.24.0 插桩、V3 telemetry、Prometheus/TCP、GPU/资源监控、安全启动与 watchdog |
| `claude-MCP` | `/home/claude-MCP/mcpmark`（工作目录） / `/home/claude-MCP/claude-MCP`（Git 副本） | Claude Code + 模型中转站驱动 MCPMark 工作负载；MCP 服务 runner、任务定义、流量分析 |
| `claude-GAIA` | `/home/claude-GAIA` | Claude Code + 模型中转站驱动 GAIA workload；任务执行与结果绘图 |

## 逻辑关系

```text
Codex 主链：
codex_qwen_GAIA  ->  codex_qwen  ->  waitung-inference-server-measurement

Claude 侧（并列，独立运行）：
claude-MCP   （MCPMark workload）
claude-GAIA  （GAIA workload）
```

这不是严格的单向依赖：`codex_qwen_GAIA` 同时通过 `ssh spk` 直接调用服务端
`scripts/spark_capture.py`，并把 `server_traffic.pcapng` / `server_timeline.jsonl`
交给 `codex_qwen` 解析。Claude 两个仓库不共享 Codex 的测量运行时，但可作为
不同 Agent/模型/中转站的对照工作负载。

## 真实接口

| 接口 | 说明 |
|---|---|
| `ssh spk` | Codex runner -> 推理服务器；18000 端口 SSH 隧道承载模型 HTTP |
| `spark_capture.py worker --path ... --max-seconds ... --metrics-interval --tcp-interval --detailed-events` | 启动服务端采集 |
| `spark_capture.py collect --run-id --task-id` | 回收服务端数据 |
| `agent-traffic-v3.0` | 服务端 telemetry JSONL 协议（`events-<pid>-NNN.jsonl`） |
| 七文件结果 | `task_prompt.txt`, `codex_output.json`, `codex_timeline.jsonl`, `result.json`, `raw_traffic.pcapng`, `server_traffic.pcapng`, `server_timeline.jsonl` |

## 部署顺序（Codex 主链）

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

Claude 两个仓库按各自 README 独立部署，不要求 Codex 主链启动。

## 版本基线

| 组件 | 版本 |
|---|---|
| vLLM | 0.24.0（5 个 overlay 文件 + `vllm-0.24.0-telemetry.patch`） |
| Codex CLI | 0.169.0 |
| Claude Code CLI | 本地模型中转站接入 |
| Python（本机） | 3.14.4 |
| Python（服务端 vLLM venv） | 3.13 |
| 测量协议 | `agent-traffic-v3.0` |
| 模型 | Qwen3.6-35B-A3B，context 32768 |
| Exa MCP | `https://mcp.exa.ai/mcp?tools=web_search_exa`（streamable HTTP，匿名） |
| MCPMark 上游 | `eval-sys/mcpmark` @ `cd45b7f` |

## 当前基线 commit

| 仓库 | 当前 HEAD |
|---|---|
| codex_qwen | `0742a268d5f7cb889fc19f5a1cbc9327c0cd1016` |
| codex_qwen_GAIA | `c74624cc0f6a49190f361bcb1b139f61bf84c438` |
| waitung-inference-server-measurement | `bb929057f201be1a2d085350d02a18dc642de521` |
| claude-MCP | `45b7115a951e6e5f42c0734667f48f995a32aada` |
| claude-GAIA | `de21ccf1786715253c779a49cc0ed6c90f5fa364` |

> 首次导入 commit：codex_qwen `73589fb`、codex_qwen_GAIA `270a442`、
> server `bb92905`；后续仅追加文档/命名修正，未改写历史。
