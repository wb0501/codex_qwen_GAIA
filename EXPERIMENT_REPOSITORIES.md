# EXPERIMENT_REPOSITORIES — Agent Traffic Measurement 代码仓库

本项目的代码按职责拆分为 5 个独立 GitHub Public Repository。Codex 侧两个
workload 仓库并列，Claude 侧两个 runner 也并列；服务端测量仓库被两侧共享。

## 仓库职责

| 仓库 | 路径 | 职责 |
|---|---|---|
| `codex_qwen` | `/home/codex_qwen` | HumanEval workload + 可复用测量框架（PCAP/HTTP/SSE/TCP/Prometheus/关联分析） |
| `codex_qwen_GAIA` | `/home/codex_qwen_GAIA` | GAIA workload + **自带测量副本**（spark_measurement.py、scripts/experiment_config.py、scripts/telemetry_merge.py、config/spark） |
| `waitung-inference-server-measurement` | `/home/huanglabspk/WaiTung`（DGX Spark） | vLLM 0.24.0 插桩、V3 telemetry、Prometheus/TCP、GPU/资源监控、安全启动与 watchdog |
| `claude-MCP` | `/home/claude-MCP/mcpmark`（工作目录） / `/home/claude-MCP/claude-MCP`（Git 副本） | Claude Code + 模型中转站驱动 MCPMark |
| `claude-GAIA` | `/home/claude-GAIA` | Claude Code + 模型中转站驱动 GAIA |

## 并列关系，不是链式依赖

`codex_qwen` 与 `codex_qwen_GAIA` 是两个独立 workload：

```text
codex_qwen       (HumanEval + measurement)  --ssh spk-->  waitung-inference-server-measurement
codex_qwen_GAIA  (GAIA + vendored measurement) --ssh spk-->  waitung-inference-server-measurement
```

- `codex_qwen_GAIA` 不再 import `/home/codex_qwen` 的任何 Python 模块。
- 它自带 `spark_measurement.py`、`scripts/experiment_config.py`、`scripts/telemetry_merge.py`、`config/spark/`。
- 两个 Codex 仓库各自通过 `ssh spk` 调用服务端 `scripts/spark_capture.py`，互不调用。
- 服务端是共享数据源，不 import 任何客户端仓库。

Claude 侧两个仓库同样独立，不与 Codex 侧共享运行时：

```text
claude-MCP    (MCPMark workload)   --model relay-->  Claude
claude-GAIA   (GAIA workload)      --model relay-->  Claude
```

## 真实接口

| 接口 | 说明 |
|---|---|
| `ssh spk` | 两个 Codex runner 各自建立；18000 端口 SSH 隧道承载模型 HTTP |
| `spark_capture.py worker --path ... --max-seconds ... --metrics-interval --tcp-interval --detailed-events` | 启动服务端采集 |
| `spark_capture.py collect --run-id --task-id` | 回收服务端数据 |
| `agent-traffic-v3.0` | 服务端 telemetry JSONL 协议 |
| 七文件结果 | task_prompt / codex_output / codex_timeline / result / raw_traffic / server_traffic / server_timeline |

## 部署顺序

1. **推理服务端**：`waitung-inference-server-measurement`
   - 部署 `scripts/` 与 `vllm_overlay/`
   - `scripts/start_vllm.sh --start --context-window 32768 --detailed-events 1`
   - 确认 `/health` 200、guard armed、watchdog 运行
2. **任一 Codex workload**（独立选择）
   - `codex_qwen`：HumanEval 入口 + 测量框架
   - `codex_qwen_GAIA`：`cp config/ssh_config.example config/ssh_config`，填入本机 SSH 连接；`python run_gaia_codex.py --check-tools`
3. **Claude 侧**（独立）
   - 按 `claude-MCP` / `claude-GAIA` 各自的 README 配置模型中转站

## 版本基线

| 组件 | 版本 |
|---|---|
| vLLM | 0.24.0（5 个 overlay 文件 + vllm-0.24.0-telemetry.patch） |
| Codex CLI | 0.169.0 |
| Claude Code CLI | 本地模型中转站接入 |
| Python（本机） | 3.14.4 |
| Python（服务端 vLLM venv） | 3.13 |
| 测量协议 | agent-traffic-v3.0 |
| 模型 | Qwen3.6-35B-A3B，context 32768 |
| Exa MCP | https://mcp.exa.ai/mcp?tools=web_search_exa |
| MCPMark 上游 | eval-sys/mcpmark @ cd45b7f |

## 当前基线 commit

| 仓库 | commit |
|---|---|
| codex_qwen | `0742a268d5f7cb889fc19f5a1cbc9327c0cd1016` |
| codex_qwen_GAIA | `9ae48488ada530a0585311b072a7b2c5f2a86a33` |
| waitung-inference-server-measurement | `0468ceda12223a91f476b95fc12e7645b858685f` |
| claude-MCP | `6eac2e173ac72e474512da0153a60ff521ae147a` |
| claude-GAIA | `de21ccf1786715253c779a49cc0ed6c90f5fa364` |

后续提交请以 `git rev-parse HEAD` 为准；上表是本次解耦前的基线。