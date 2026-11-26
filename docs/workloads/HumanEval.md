# Workload: HumanEval

HumanEval 是此前使用过的工作负载，用于框架回归与对照实验。

## 在本框架中的位置

- Runner: `run_humaneval_codex.py`（位于
  `waitung-agent-traffic-measurement` 仓库根目录）
- 用途：验证采集链路、Codex 集成与 V3 telemetry 在单轮代码生成任务上的行为
- 当前不是主力 workload

## 与 GAIA 的差异

| 项 | GAIA | HumanEval |
|---|---|---|
| 任务来源 | GAIA 2023 validation Parquet | HumanEval JSONL |
| 工具使用 | 允许联网检索与 Shell | 以代码生成与本地执行为主 |
| 答案校验 | 严格文本/数值比较 | 单元测试通过 |
| 交互轮次 | 多轮工具调用 | 通常较少 |

## 复用方式

两个 adapter 共用同一套测量框架、七文件结果结构与双端采集调用，
因此新增 workload 时只需实现"读取任务 + 驱动 Agent + 校验答案"三段，
采集与结果封装保持复用。
