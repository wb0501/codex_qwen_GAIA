# Workload: GAIA

GAIA 是当前主力 workload：Agent 需要在浏览器/Shell 工具帮助下回答
带唯一答案的研究型问题。

## 在本框架中的位置

- Adapter: `run_gaia_codex.py`
- 数据集: `DATASET/GAIA/2023/validation/metadata.parquet`（不入库）
- 任务数: 165（validation split）
- 答案校验: GAIA legacy numeric/list/text comparison，严格匹配
  `FINAL ANSWER:` 末行；不使用 LLM judge

## 单任务执行链

```text
任务读取 -> 临时 CODEX_HOME -> bwrap 隔离 -> 启动 dumpcap
        -> 启动 Codex（codex exec --json）
        -> 模型请求经 exa_tool_gateway -> SSH 隧道 -> vLLM
        -> 通过 ssh spk 启动 spark_capture.py worker
        -> 结束后回收双端 PCAP、服务端 timeline、V3 telemetry
        -> 写出七文件 -> 答案校验 -> 批次状态机
```

## 工具访问

- Exa MCP：`web_search_exa`（匿名，直连 `mcp.exa.ai`，进程级 NO_PROXY）
- 本机 Shell：`curl` / `wget` / Python HTTP，经 mitmproxy 与 Windows 上游代理
- 未被调用的工具名（如 `web_search`、`mcp__exa`）记为 `unsupported call`，
  不计为成功 MCP 调用

## 常用命令

```bash
python run_gaia_codex.py --check-tools
python run_gaia_codex.py --all --list-tasks
python run_gaia_codex.py --task <task_id> --level 1 --capture-server --timeout 900
python -u run_gaia_codex.py --all --timeout 900
```

## 单任务超时

`--timeout 900` 是**单任务**上限，不是批次上限。超时任务由批次机制记录为
`timed_out` 并继续执行后续任务。
