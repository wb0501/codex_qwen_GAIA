# waitung-agent-workload-runners

Agent 测量实验的**工作负载执行框架**：启动 Agent CLI、调度 Benchmark 任务、
接入公共测量框架、记录结果。

本仓库属于 **Agent Traffic Measurement** 研究项目。它本身不定义具体
Benchmark，而是通过 workload adapter 驱动不同的 Agent 工作负载：

| Workload | 状态 | 说明 |
|---|---|---|
| GAIA | 当前主力 workload | `run_gaia_codex.py`，165 个 validation 任务 |
| HumanEval | 历史 workload | 由 `waitung-agent-traffic-measurement` 仓库的 `run_humaneval_codex.py` 承接 |
| 其他 Benchmark | 预留 | 新增 adapter 见 `docs/workloads/ADDING_A_BENCHMARK.md` |

## 职责

- 通过 Codex CLI 驱动 Agent 执行任务（`codex exec --json`）
- Bubblewrap 文件系统隔离与逐任务临时 `CODEX_HOME`
- 任务选择、断点续跑、超时与批次状态机
- 调用公共框架完成双端 PCAPNG、Prometheus、TCP 采集
- 七文件结果输出与答案校验
- 进程级 NO_PROXY、TLS Inspection、Windows 上游代理配置

## 目录

```text
run_gaia_codex.py     GAIA workload adapter + 批次执行器
exa_tool_gateway.py   Responses API 工具名网关（namespace -> flat function）
requirements.txt      Agent 任务依赖锁定
docs/                 实验文档与 workload 说明
```

## 关键修复：Exa MCP 工具名不匹配

vLLM 0.24.0 把 Responses 的 `type=namespace` 工具平铺成以 namespace 命名的
函数（`mcp__exa`），Qwen 因此生成 `mcp__exa` 或 `web_search_exa`，而 Codex 需要
`namespace=mcp__exa` + `name=web_search_exa`。

`exa_tool_gateway.py` 在 127.0.0.1:18001 提供本地网关：

```text
Codex -> 127.0.0.1:18001 (gateway) -> 127.0.0.1:18000 (SSH) -> Spark vLLM
```

- 请求：namespace 工具展开为 `<namespace>__<tool>` 扁平函数
- 响应：`function_call.name` 拆回 `namespace` + `name`
- 仅监听回环，不修改模型、vLLM 或服务端

## 环境要求

- Python 3.13 / 3.14（基线 3.14.4）
- Codex CLI 0.169.0（`codex` 与 `codex-code-mode-host`）
- `bwrap`、`dumpcap`、`tshark`、`mitmdump`
- 公共测量框架：`waitung-agent-traffic-measurement`
- 远端推理服务：`waitung-inference-server-measurement`

## 快速开始

```bash
python -m pip install -r requirements.txt
python run_gaia_codex.py --check-tools
python run_gaia_codex.py --all --list-tasks
```

## 仓库关系

见 `EXPERIMENT_REPOSITORIES.md`。
