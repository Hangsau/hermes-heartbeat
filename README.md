# Hermes Heartbeat

**零 LLM token 消耗、全本地運行、fail-safe by design 的雙層 AI agent 自主穩態維護系統。**

生物體有自主神經系統——不需要大腦來處理心跳。你的 AI agent 也應該有。

## 一句話定位

> Hermes Heartbeat 不與 LangSmith/AgentOps 競爭 observability 市場；它在創造一個新市場：**AI agent 的自主穩態維護（autonomic homeostasis）。**

## 架構

```
自主神經層（每 30 秒）                認知循環層（每 30 分鐘，僅空閒時）
├── 系統快照（disk/mem/session）      ├── 五大行動評分（WORK/CONNECT/EVOLVE/REPORT/REST）
├── stuck agent 偵測                  ├── 選擇最高分行動 → 執行
└── 寫入 heartbeat_state.json         ├── cooldown 防重複
                                       ├── fail-safe shell 操作
                                       └── 寫入 action log（學習管線）
```

## 五大閉環行動

| 行動 | 做什麼 | 安全保證 |
|------|--------|---------|
| **WORK** | 清 cache、歸檔舊 session、git push | 只清 > 7 天檔案、不 force push |
| **CONNECT** | provider 降級 → pause 對應 cron job | pause 只阻止下次排程、可逆 |
| **EVOLVE** | 跑 tests、檢查 package 更新、掃 cron errors | pytest 只跑自己的 tests、pacman --dry-run |
| **REPORT** | 有行動才報到 Telegram 摘要 | 不推裸 metrics、不堆「一切正常」 |
| **REST** | 什麼都不做（系統健康） | 休息不是問題的設計 |

## 快速開始

```bash
# 試跑一次（dry run，不執行動作）
python3 heartbeat_v2.py --dry-run

# 執行特定行動
python3 heartbeat_v2.py --action=WORK

# 跑測試
python3 -m pytest test_heartbeat_v2.py -v
```

## 依賴

- Python 3.10+
- `pytest` + `pytest-cov`（僅 EVOLVE canary test）
- 無外部 API 依賴、無 LLM token 消耗、無資料庫
- 設計為整合在 Hermes agent 環境中（`~/.hermes/`）

## 競品對比

| 競品 | 做什麼 | 自主修復？ |
|------|--------|:---:|
| LangSmith / AgentOps / Arize | Observability（看問題） | ❌ |
| OpenCrabs (744⭐) | Agent 改自己的提示詞 | 半（改行為，非修環境） |
| SRE-Agent (66⭐) | 修 K8s Pod | 是（修基礎設施，非 agent 自己） |
| OpenClaw heartbeat-helper | 固定階梯式重啟 gateway | 是（階梯，非動態選擇） |
| **Hermes Heartbeat** | **Agent 自主維運（disk/cron/provider/session）** | ✅ scoring-based 動態選擇 |

## 專案狀態

- Phase 3：五大閉環行動 ✅（912 行 code，53 tests，0.20s）
- Phase 4：學習管線完善（進行中）
- Phase 5：多 agent + chaos engineering
- Phase 6：生態化（獨立 package + plugin system）

## 授權

MIT
