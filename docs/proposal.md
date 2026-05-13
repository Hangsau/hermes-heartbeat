# Hermes Heartbeat：AI Agent 自主穩態維護系統 — 專案計畫書

> **文件日期**：2026-05-14  
> **版本**：v1.0  
> **作者**：Hermes Heartbeat Team（Hang Yeh + Hestia）  
> **對應程式碼**：`~/.hermes/scripts/heartbeat_v2.py`（912 行）  
> **測試**：`test_heartbeat_v2.py`（53 tests, 0.20s）  
> **授權**：MIT（暫定）

---

## 一、專案定義與願景

### 一句話

> **Hermes Heartbeat 不與 LangSmith/AgentOps 競爭 observability 市場；它在創造一個新市場：AI agent 的自主穩態維護（autonomic homeostasis）。**

### 是什麼

一個**零 LLM token 消耗、全本地運行、fail-safe by design** 的雙層健康管理系統，讓 AI agent 像生物體一樣維持自身穩態——偵測問題、自主選擇行動、執行修復、記錄學習、只在有必要時摘要報告。

### 不是什麼

- ❌ 不是監控儀表板（不會畫圖、不推 alert）
- ❌ 不是 auto-scaling（不管理基礎設施）
- ❌ 不是 LLM-based agent（純 shell/fs 操作，不消耗 token）
- ❌ 不是 cron scheduler（雖然利用 cron 觸發，但決策邏輯不是排程）

### 設計哲學

```
生物的自主神經系統不是「叫大腦來處理心跳」
——它自己會處理。Hermes Heartbeat 就是 AI agent 的自主神經系統。
```

- **自主 ≠ 自動**：不是預設 script 的定時任務，而是根據系統狀態動態選擇行動
- **零 token 消耗**：所有 sensor/actuator 都是系統級操作，不呼叫 LLM
- **Fail-safe by design**：最壞情況是「什麼都不做」，而非「做了錯誤的事」
- **有行動才報告**：不像傳統監控把所有 metrics 推出去，只報「做了什麼 + 學到什麼」

---

## 二、解決什麼問題

### 核心痛點：AI Agent 的「無人看管」風險

長期運行的 AI agent（如 Hermes）面臨一系列緩慢累積的健康問題：

| 問題 | 症狀 | 不處理的後果 |
|------|------|------------|
| **磁碟蠶食** | browser/pip cache 累積、tmp 殘留、舊 session 檔案 | 跑幾個月後 / 滿了，所有東西 crash |
| **Provider 降級** | openrouter/deepseek API 開始回 429/503 | 排程的 cron job 連續失敗，任務積壓 |
| **Session 堆積** | 閒置數週的 agent session 不清理 | gateway 負擔增加、memory 浪費 |
| **程式碼腐化** | 測試被改壞但沒人知道、package 過時 | 某天發現核心功能早就壞了 |
| **Cron 靜默失敗** | cron job 連續 error 但沒有人類在監控 | 積累數天的失敗，恢復困難 |
| **資訊噪音** | 每 30 分鐘推 `disk=12.9% mem=8.3%` 到 Telegram | 人類學會無視心跳訊息，真正問題被淹沒 |

### 現有方案的不足

目前的 AI agent 生態系中，所有工具都在做 **observability（可觀測性）**：

- **LangSmith / AgentOps / Arize / Braintrust**：告訴你「出問題了」，不幫你「修問題」
- **傳統監控（Prometheus + Alertmanager）**：推 alert 到人類，人類決定要不要修
- **Cron + shell script**：能做固定任務，但不會根據系統狀態動態選擇行動

**沒有人在做：偵測 → 自主決策 → 執行修復 → 記錄學習 → 彙總報告**。

### Hermes Heartbeat 填的洞

```
傳統監控：   Sensor → Alert → Human → Fix
Heartbeat：  Sensor → Score → Decide → Act → Record → Report (only if acted)
```

人類從「必須在線的維運人員」變成「每幾天看一次摘要的監督者」。

---

## 三、架構總覽

### 3.1 雙層設計（借鑒神經科學）

```
自主神經層（Autonomic Layer）          認知循環層（Cognitive Layer）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
觸發頻率：每 30 秒                    觸發頻率：每 30 分鐘（僅空閒時）
功能：                               功能：
  ├─ get_snapshot()                   ├─ 檢查是否在認知間隔內
  │   ├─ disk_usage()                 ├─ score_actions()
  │   ├─ memory_usage()               │   ├─ WORK（cache/session/git）
  │   ├─ active_sessions()            │   ├─ CONNECT（provider health）
  │   ├─ failed_platforms()           │   ├─ EVOLVE（self-check）
  │   └─ stuck_agents()               │   ├─ REPORT（summary）
  ├─ 寫入 heartbeat_state.json        │   └─ REST（do nothing）
  ├─ 偵測 stuck agent → send_interrupt│  ├─ select_action() → 選最高分
  └─ 計算 warmth 建議                 ├─ check_cooldown() → 避免重複行動
                                      ├─ execute_action()
                                      ├─ _record_action_log() → 寫入 jsonl
                                      └─ 若 REPORT 且有事 → Telegram 摘要
```

**設計原理**：類似人體的 sympathetic/parasympathetic + prefrontal cortex 分工。自主神經層只感知不決策（快、便宜、不能關掉），認知層只在空閒時介入（慢、有判斷力、只在需要時啟動）。

### 3.2 核心資料結構

#### HeartbeatSnapshot
```python
@dataclass
class HeartbeatSnapshot:
    ts: float                          # Unix timestamp
    disk_free_pct: float               # 磁碟可用 %
    memory_used_pct: float | None      # 記憶體使用 %
    active_sessions: int               # 活躍 agent session 數
    stuck_agents: list[str]            # 卡住的 agent ID
    failed_platforms: list[str]        # degraded provider (openrouter/opencode...)
    queue_depth: int                   # 積壓事件數
    cache_size_mb: float               # ~/.cache/ 大小
    action_history: list[dict]         # 歷史行動記錄
    kanban_ready: int                  # 就緒的 kanban 任務數
```

#### Action Log Entry（每行動一條）
```json
{
  "ts": "2026-05-13T22:00:00+08:00",
  "action": "WORK",
  "trigger": {"disk_pct": 12.9, "memory_pct": 8.3},
  "steps": [
    {"op": "cache_clean", "result": "removed 1.8GB", "ok": true},
    {"op": "archive_sessions", "count": 6, "ok": true},
    {"op": "git_push", "repo": "managed-agents-research", "result": "no unpushed", "ok": true}
  ],
  "outcome": "ok",
  "errors": [],
  "learnings": ""
}
```

---

## 四、五大閉環行動詳解

### 4.1 WORK — 系統維護

| 項目 | 內容 |
|------|------|
| **觸發條件** | 磁碟可用 < 85%、cache > 128MB、或長時間未執行 |
| **行動** | 1. 清 `~/.cache/` 超過 7 天的檔案<br>2. 清 `/tmp/hermes_*` 超過 1 天的殘留<br>3. 歸檔閒置 > 168h 的舊 session<br>4. Git push（僅 fast-forward） |
| **安全性** | 只用 `-mtime +7` 清舊檔；session 歸檔需 `-mmin +60` 確保沒人在寫；git 不 force push |
| **不做事** | 不重啟 gateway、不改 config、不強制清理 |

### 4.2 CONNECT — Provider 降級自動處理

| 項目 | 內容 |
|------|------|
| **觸發條件** | `snap.failed_platforms` 非空 或 cron output 中有 429/timeout 錯誤 |
| **行動** | 1. 讀 cron jobs 列表 → 找出用 degraded provider 的 job<br>2. 自動 pause 這些 job<br>3. provider 恢復 → unpause |
| **安全性** | pause 只阻止下次排程，不中斷正在跑的工作；加 cooldown 防止 flicker 重複操作 |
| **不做事** | 不自動換 provider、不改 job config |

### 4.3 EVOLVE — 自我檢查

| 項目 | 內容 |
|------|------|
| **觸發條件** | 上次 EVOLVE > 6 小時前 |
| **行動** | 1. `pytest test_heartbeat_v2.py -v --tb=short` → 記錄失敗<br>2. `pacman -Sy --dry-run` → 記錄可用更新<br>3. `_scan_cron_errors()` → 記錄有問題的 cron job |
| **安全性** | 測試只跑自己的 test suite；pacman 只用 dry-run；cron scan 只讀最後一個 output |
| **不做事** | 不自動修測試、不自動 update package、不自動修 cron |

### 4.4 REPORT — 有意義的摘要

| 項目 | 內容 |
|------|------|
| **觸發條件** | 認知循環觸發（每 30 分鐘） |
| **行動** | 讀今天的 `heartbeat_action_log.jsonl` → 建摘要 → 推送 Telegram |
| **格式** | 只列「做了什麼」+「學到什麼」；如果今天什麼都沒做 → silent |
| **不做事** | 不推裸 metrics、不堆「一切正常」訊息 |

### 4.5 REST — 休息

| 項目 | 內容 |
|------|------|
| **觸發條件** | 系統健康＋無待辦事項 |
| **行動** | 清超過 7 天的舊 health logs |
| **意義** | 對一個自主系統來說，**休息不是問題的設計**——有能力判斷「現在沒事做」本身就是健康 |

---

## 五、學習管線

### 5.1 行動記錄（Action Log）

每個行動執行完，寫入 `~/.hermes/heartbeat_action_log.jsonl`。每條包含：
- `trigger`：觸發條件（當時的 disk/mem/cron 狀態）
- `steps`：具體做了什麼（可復現的步驟清單）
- `outcome`：ok / partial / failed
- `errors`：錯誤清單
- `learnings`：人類可讀的學習筆記（由 `learning-extraction` skill 定期餵入）

### 5.2 模式提取（Phase 4 方向）

`heartbeat_action_log.jsonl` 可被 `learning-extraction` skill 定期處理：

```
日誌 → 找出 pattern（如 "opencode 連續三天 429"）→ 生成建議（"換 provider 或降頻"）→ 下次 REPORT 附上建議
```

### 5.3 反思迴圈

```
行動 → 記錄 → 下次見面輕描淡寫說一聲 → 人類調整策略（如加 cooldown、換 provider）→ 行動策略進化
```

---

## 六、產出效果

### 6.1 立即效果（Phase 3 已完成）

| 效果 | 量化 |
|------|------|
| 磁碟自動清理 | 每次 WORK 清 0~2GB，防止磁碟滿 |
| Provider 自動降級 | degraded 後自動 pause 對應 cron，避免 429 迴圈 |
| 自我測試 | 每次 EVOLVE 跑 53 tests，0.20s，程式碼腐化會被立刻發現 |
| Cron 錯誤自動掃描 | 掃 1693 個 output file，找出有問題的 job（實際已有 3 個被發現） |
| Telegram 噪音消除 | 有行動才報，預計從 48 條/天降至 0~3 條/天 |

### 6.2 長期價值

1. **信任積累**：人類不需要每天檢查；信任系統會自己處理日常維運
2. **問題視覺化**：action log 提供時間軸上的健康軌跡，可回溯「什麼時候開始出問題」
3. **策略進化**：從 log 中學到哪些行動有效、哪些無效，逐步優化行動選擇
4. **可複製性**：雙層架構是抽象的，可移植到任何長期運行的 agent 系統

---

## 七、缺點與風險

### 7.1 已知缺點

| 缺點 | 影響 | 緩解措施 |
|------|------|---------|
| **無 session 隔離** | WORK 的 git push 和 EVOLVE 的 pytest 跑在同一環境 | 不影響正確性，但可能互相汙染（如 git push 失敗時 pytest 也看到髒狀態） |
| **無狀態鎖定** | 認知層 30 分鐘才跑一次，可能錯過快速變化的問題 | 自主神經層 30s 掃描可捕獲大部分問題；極端 case 交給 Phase 4 |
| **無 human-in-loop 介入通路** | 如果系統誤判（如 pause 了不該 pause 的 job），沒有人類即時介入 | 所有操作都是可逆的（pause 可 unpause、archive 可移回）；REPORT 提供回溯 |
| **provider mapping 手動維護** | CONNECT 的 provider→cron job 對應需要手動更新 | 目前 job 數量少（< 10 個），手動 mapping 可行 |
| **無跨 agent 協調** | 只有一個 Hermes 實例，無法處理多 agent fleet | Phase 5 方向 |
| **學習管線弱** | 目前只記錄不行動；learning-extraction 未完成 | Phase 4 方向 |
| **磁碟清理不智慧** | 固定 7 天閾值，不根據可用空間動態調整 | 目前 disk 壓力不大，固定閾值夠用 |
| **無 Chaos 測試** | 不知道在極端情況下（如 disk 99%）的行為 | 可手動模擬；formal chaos engineering 留給 Phase 5 |

### 7.2 風險矩陣

| 風險 | 機率 | 衝擊 | 策略 |
|------|:---:|:---:|------|
| `_safe_shell` 在極端 disk 滿時卡死 | 低 | 中 | 所有 shell 命令設 timeout=30s |
| pause/unpause race condition（兩個 cognitive cycle 同時操作） | 低 | 低 | 目前的 cron job 數量少；可加 file lock |
| action log 無限增長 | 中 | 低 | REPORT 只讀今天；log rotation 留 Phase 4 |
| provider mapping 過時導致不 pause 應該 pause 的 job | 中 | 中 | REPORT 會顯示 unmapped provider 的錯誤 |

---

## 八、競品分析（2026-05-14 擴充版）

> **搜尋策略**：8 組 GitHub API 查詢 + 4 組 web search + 3 個重點 repo deep-dive。涵蓋 self-healing、watchdog、health monitor、lifecycle、resilience、fault tolerance、autonomic computing、staged recovery 等關鍵詞組合。

### 8.1 市場全景：Observability 飽和，Autonomy 真空

現有的 AI agent 監控/管理工具分為四層，**前三層全部落在 observability 象限**：

| 層級 | 代表產品 | 核心功能 | 自主修復？ |
|------|---------|---------|:---:|
| **LLM Observability** | LangSmith, Arize, Braintrust | Trace/debug/評估/prompt versioning | ❌ |
| **Agent 專用監控** | AgentOps, `claw-agent-dashboard` (26⭐) | Session replay/cost tracking/compliance | ❌ |
| **基礎設施監控** | Prometheus + Alertmanager | Metrics → alert → pager | ❌ |
| **Agent Fleet 儀表板** | `agentpulse` (0⭐) | Datadog-like dashboard for agent fleets | ❌ |
| **GitHub 開源框架** | skrun, continuum, smolagents, Swarm | 任務執行 | ❌（無健康概念） |
| **OpenClaw 生態** | OpenClaw heartbeat/cron | Daemon-based 心跳 + 隔離 cron | ❌ |

### 8.2 少數「自稱自癒」的專案 — 但都不是 agent 自身維護

#### OPENCRABS（744⭐）— 自我改進，非自我修復
- **語言**：Rust 單一二進位檔（26MB）
- **自癒機制**：RSI（Recursive Self-Improvement）— Agent 自主修改自己的 brain 檔案（提示詞），基於成功/失敗的回饋帳本
- **核心問題**：修復對象是 **Agent 的行為策略**，不是 **Agent 的運行環境**
- **vs Hermes**：OpenCrabs 優化「如何回答」，Hermes 維護「能否運行」。層級不同，不競爭。

#### SRE-AGENT-APP（66⭐）— 基礎設施自癒，非 agent 自癒
- **語言**：Java 17 / Spring Boot 3 / LangChain4j
- **自癒機制**：OODA 決策迴圈（Observe→Orient→Decide→Act），修復 K8s Pod 的 CrashLoopBackOff
- **核心問題**：修復對象是 **Kubernetes 基礎設施**，不是 **AI agent 自身的健康**
- **vs Hermes**：不同領域。SRE-Agent 是 AIOps 工具，Hermes 是 agent autonomic system。

#### LEDGERMIND（14⭐）— 最接近 Hermes Heartbeat 的設計
- **語言**：Python 3.10+ / MCP 協議
- **自癒機制**：5 分鐘背景 worker 循環 — Git 同步 → 反思 → 記憶衰減 → SQLite 索引自癒重建
- **知識生命週期**：PATTERN → EMERGENT → CANONICAL 三階段演化
- **衝突解決**：向量相似度 ≥70% 自動超集，50-70% LLM 豐富化
- **vs Hermes**：兩者都追求自主知識管理，但 LedgerMind 專注於記憶層（knowledge lifecycle），Hermes Heartbeat 專注於維運層（disk/cron/provider/session）。**互補而非競爭。**

#### heartbeat-helper-for-openclaw（0⭐）— 設計模式最接近
- **語言**：純 shell script
- **自癒機制**：**階梯式恢復（Staged Recovery Ladder）** — 連續失敗次數決定行動強度
  - 1st failure → log
  - 2nd → restart gateway + app
  - 3rd → `openclaw doctor --diagnose`
  - 4th → `openclaw doctor --fix --yes`
- **特色**：DRY_RUN 模式、鎖定目錄避免並行、持久化失敗計數
- **vs Hermes**：**這是目前找到最接近 Hermes Heartbeat 設計模式的專案。** 都是 5 分鐘級別的健康檢查 + 自主行動 + dry run + 狀態持久化。但它是固定階梯（if failure count = N → action），Hermes 是 scoring-based 動態選擇。Hermes 的設計更靈活、更通用。

### 8.3 搜尋覆蓋率總結

| 搜尋關鍵詞 | GitHub 總結果 | 有效自癒專案 | 與 Hermes 重疊 |
|-----------|:---:|:---:|:---:|
| `self-healing agent autonomous` | 376 | 9/10 | 無（都是 agent 行為改進或基礎設施修復） |
| `agent watchdog autonomous recovery` | 1 | 1/1 | 部分（watchdog 概念） |
| `agent health monitor self` | 25 | 5/10 | 間接（多為純監控） |
| `agent lifecycle management autonomous` | 28 | 3/10 | 低（DevOps 領域） |
| `autonomic computing agent self-managing` | **0** | — | 無（學術概念未落地） |
| `agent resilience fault tolerance autonomous` | **0** | — | 無（分散式系統概念未跨入 agent 領域） |
| `staged recovery agent` | **0** | — | 無（僅 OpenClaw shell script） |
| `openclaw heartbeat health` | 3 | 3/3 | **最接近**（OpenClaw 生態有三個自癒 helper） |

**三個「0 結果」查詢最有意義**：`autonomic computing`、`fault tolerance`、`staged recovery` 這些在傳統分散式系統中成熟的術語，在 AI agent 領域**完全沒有人用**。這不是搜不到——是真的沒人做。

### 8.4 「Self-Healing」在不同專案中的語意漂移

```
OpenCrabs:   Self-healing = Agent 改進自己的提示詞
SRE-Agent:   Self-healing = Agent 修復 K8s Pod
LedgerMind:  Self-healing = Agent 修復自己的記憶儲存層
Heartbeat:   Self-healing = Agent 修復自己的運行環境（disk/cron/provider/session）
OpenClaw:    Self-healing = Shell script 重啟 gateway（固定階梯）
```

**結論**：「Self-Healing」是一個被過載的術語，在不同專案中指向完全不同的事物。Hermes Heartbeat 是唯一一個以「**運行環境維護**」為核心自癒對象的系統。

### 8.5 Hermes Heartbeat 的獨特定位

```
                         Observability（看問題）
                              │
         LangSmith ───────────┼────────── AgentOps
         Arize                 │           Braintrust
         Prometheus            │           claw-agent-dashboard
                              │
         ─────────────────────┼────────────────────→ Autonomy（修問題）
                              │
         OpenCrabs (改 prompt) │     ★ Hermes Heartbeat (修環境)
         SRE-Agent (修 K8s)    │     ★ LedgerMind (修記憶) ← 互補
         OpenClaw helper (階梯) │
```

### 8.6 一句話定位

> **在 AI agent 自主運行環境維護這個細分領域，Hermes Heartbeat 沒有競品——它是第一個。</strong>OpenCrabs 改行為、SRE-Agent 修 K8s、LedgerMind 修記憶、OpenClaw helper 固定階梯。只有 Hermes Heartbeat 做 scoring-based 動態選擇的運行環境閉環維護。</strong>

---

## 九、市場定位與潛在價值

### 9.1 目標用戶

| 用戶 | 場景 |
|------|------|
| **長期運行的個人 AI agent** | 像 Hermes 這樣 24/7 跑的 agent，人類不想每天檢查 |
| **開源 agent 框架** | 作為內建的健康層（類似 Kubernetes 的 kubelet） |
| **Agent-as-a-Service 平台** | 每個 tenant agent 自帶心跳，降低維運成本 |
| **邊緣 AI 裝置** | 無人看管的 edge device 上的 agent 需要自癒能力 |

### 9.2 為什麼現在做

1. **AI agent 正在從 "session-based" 走向 "always-on"**：越來越多的 agent 不是跑一次就關掉，而是持續運行數週數月
2. **Provider reliability 是系統性問題**：openrouter/deepseek/openai 都常降級，agent 需要能自己處理
3. **可觀測性市場已經飽和**：LangSmith/AgentOps 佔滿了這個空間，但自主修復是藍海
4. **神經科學啟發的設計是新方向**：現有工具都是軟體工程的思維（monitor→alert→ticket），還沒人從生物學角度設計 agent 健康系統

### 9.3 可複製性

架構本身是抽象的：
- `HeartbeatSnapshot` 可以用任何系統指標替換
- `score_actions()` / `select_action()` 是純函數，可獨立測試
- `execute_action()` 的 action function 可以根據不同 agent 需求替換
- 整個系統不到 1000 行 Python，容易理解與移植

---

## 十、未來迭代路線圖

### Phase 4：學習管線完善（預估 2-3 週）

| 項目 | 內容 |
|------|------|
| **learning-extraction 整合** | 定期處理 `heartbeat_action_log.jsonl`，用 Jaccard similarity 去重，提煉 pattern |
| **action log rotation** | 超過 30 天的 log 自動歸檔或壓縮 |
| **動態閾值** | cache clean 的 7 天閾值根據實際 disk 壓力動態調整 |
| **provider recovery 自動檢測** | 不只被動等 `failed_platforms` 清空，定期主動 probe degraded provider |
| **自我測試覆蓋率追蹤** | EVOLVE 記錄 coverage % 變化，下降時標記 |

### Phase 5：跨 Agent 與 Chaos（預估 1-2 月）

| 項目 | 內容 |
|------|------|
| **Multi-agent health mesh** | 多個 agent 之間交換 health state，互相備援 |
| **Chaos engineering for agents** | 刻意注入故障（假 429、假 disk 滿、corrupted context）→ 驗證自癒能力 |
| **Session 隔離** | 利用 git worktree 隔離 EVOLVE 的 pytest 和 WORK 的 git push |
| **Structured health protocol** | 定義 agent health state 的標準格式，讓不同 agent 框架可以互通 |

### Phase 6：生態化（預估 3-6 月）

| 項目 | 內容 |
|------|------|
| **獨立 repo** | 從 Hermes 拆出，變成可獨立安裝的 Python package |
| **Plugin system** | 允許使用者自訂 sensor 和 action，不綁定 Hermes 生態 |
| **Health dashboard** | 可選的輕量 Web UI（不違背零 token 原則，純前端） |
| **Integration docs** | 文件化如何將 Heartbeat 整合進其他 agent 框架 |

---

## 十一、指標與成功定義

### 11.1 技術指標

| 指標 | 目標 | 現狀 |
|------|------|------|
| 測試覆蓋率 | > 60% | 35%（Phase 3 新增後待測） |
| 測試執行時間 | < 1s | 0.20s（53 tests）✅ |
| 每週人為介入次數 | < 3 次 | 待追蹤 |
| 誤報率（pause 了不該 pause 的 job） | 0 | 0（Phase 3 剛上線） |
| Cron error 發現時間 | < 1 小時 | 即時（30 分鐘）✅ |

### 11.2 行為指標

| 指標 | 定義 |
|------|------|
| **自主率** | 有多少問題是系統自己修、不經人類的 |
| **靜默率** | REPORT 中「今天沒事做」的比例（越高代表系統越穩定） |
| **行動有效性** | action 執行後，相關 sensor 讀數是否改善（如 disk 清完後可用空間上升） |

---

## 十二、附錄

### A. 檔案清單

| 路徑 | 說明 |
|------|------|
| `~/.hermes/scripts/heartbeat_v2.py` | 核心程式（912 行） |
| `~/.hermes/scripts/test_heartbeat_v2.py` | 測試（53 tests） |
| `~/.hermes/plans/heartbeat-phase3-closed-loop.md` | Phase 3 計畫書 |
| `~/.hermes/heartbeat_state.json` | 自主神經層快照 |
| `~/.hermes/heartbeat_decisions.jsonl` | 認知層決策記錄 |
| `~/.hermes/heartbeat_action_log.jsonl` | 行動記錄（學習管線輸入） |
| `~/.hermes/skills/automation/heartbeat-competitive-landscape/` | 競品研究 skill |
| `~/.hermes/skills/automation/heartbeat-reporting/` | REPORT 實作 skill |
| `~/.hermes/skills/automation/heartbeat-v2-autonomous-maintenance/` | 自主維護 skill |
| `~/.hermes/skills/automation/heartbeat-test-runner/` | 測試 runner skill |

### B. 依賴

- Python 3.10+
- `pytest` + `pytest-cov`（僅 EVOLVE 的 canary test 需要）
- 無外部 API 依賴（不消耗 token、不需要 API key）
- 無資料庫（純檔案系統：JSON + JSONL）

### C. 參考文獻

- IBM Autonomic Computing Manifesto (2001) — MAPE-K loop 理論基礎
- OpenClaw heartbeat/cron architecture — 最接近的對照組
- Anthropic "Managed Agents" — 三層解耦架構參考
- Biological autonomic nervous system — 雙層設計靈感來源

### D. 競品研究來源（2026-05-14 擴充）

| 專案 | 連結 | 星數 | 定位 |
|------|------|:---:|------|
| `adolfousier/opencrabs` | [GitHub](https://github.com/adolfousier/opencrabs) | 744 | Agent 自我改進（改提示詞） |
| `qicesun/SRE-Agent-App` | [GitHub](https://github.com/qicesun/SRE-Agent-App) | 66 | K8s 基礎設施自癒（OODA） |
| `sl4m3/ledgermind` | [GitHub](https://github.com/sl4m3/ledgermind) | 14 | Agent 記憶自主管理 |
| `ek-mc/heartbeat-helper-for-openclaw` | [GitHub](https://github.com/ek-mc/heartbeat-helper-for-openclaw) | 0 | OpenClaw 階梯式恢復 |
| `danjdewhurst/openclaw-watchdog` | [GitHub](https://github.com/danjdewhurst/openclaw-watchdog) | 1 | OpenClaw watchdog |
| `ArchieIndian/openclaw-superpowers` | [GitHub](https://github.com/ArchieIndian/openclaw-superpowers) | 61 | OpenClaw 技能包（純監控） |
| `boydfd/claw-agent-dashboard` | [GitHub](https://github.com/boydfd/claw-agent-dashboard) | 26 | Agent 儀表板（被動） |
| `SurgeCLI/Surge` | [GitHub](https://github.com/SurgeCLI/Surge) | 2 | 自學習可觀測性 |

---

> **下一步**：Phase 3 已完成並上線。Phase 4（學習管線完善）待 Hang 批准後開工。
