# 新增一个 Benchmark workload

本框架把 workload（Benchmark）与测量框架分离。新增 Benchmark 只需编写
adapter，不需要改动采集链路。

## 1. 需要实现的三段

1. **任务来源**：读取任务、附件、标准答案，产出统一的 task 列表
2. **Agent 驱动**：组装 prompt，调用 `codex exec --json`，逐条消费事件写
   `codex_timeline.jsonl`（原始事件 + `codex_cli_derived` 派生事件）
3. **答案校验**：从最后一条 agent message 提取答案，与标准答案比对，
   产出 `verification` 字段

## 2. 需要复用的部分

- 临时 `CODEX_HOME` 与 Bubblewrap 隔离
- 进程级代理与 NO_PROXY（含 `mcp.exa.ai` 直连）
- `exa_tool_gateway.py`（Responses 工具名转换）
- 双端采集调用：`ssh spk` 的 `spark_capture.py worker/collect`
- 七文件结果结构
- 批次状态机、断点续跑、`--timeout` 单任务上限

## 3. 七文件结果结构

```text
task_prompt.txt
codex_output.json
codex_timeline.jsonl
result.json
raw_traffic.pcapng
server_traffic.pcapng
server_timeline.jsonl
```

## 4. 命名约定

- `run_<benchmark>_codex.py`
- 文档放 `docs/workloads/<BENCHMARK>.md`
- 在 `REPOSITORY_MANIFEST.md` 的表格中新增一行

## 5. 不要做的事

- 不要在采集链路上为单个 Benchmark 开特例
- 不要把数据集或结果提交到仓库
- 不要引入第二份 MCP / Web 日志文件
