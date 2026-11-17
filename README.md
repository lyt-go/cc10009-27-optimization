# 分布式网页抓取调度平台

面向分布式网页抓取任务调度、节点协同、请求分配、反爬策略编排、数据校验入库与抓取质量大盘的采集工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点 | `worker` | 工作节点 | 节点编号、主机地址、并发槽位数 |
| 请求分配 | `dispatch` | 分配策略 | 策略编号、目标域名、分配算法 |
| 抓取作业 | `crawljob` | 作业定义 | 作业编号、目标站点、起始URL |
| URL过滤 | `urlfilter` | 过滤规则 | 规则编号、包含模式、排除模式 |
| DNS解析 | `dnsresolver` | 解析配置 | 配置编号、域名模式、DNS服务器 |
| 内容提取 | `contentextract` | 提取器 | 提取器编号、内容类型、正文选择器 |
| 存储策略 | `storagestrategy` | 存储方案 | 方案编号、存储类型、连接参数 |
| 渲染任务 | `rendertask` | 渲染作业 | 作业编号、目标URL、浏览器版本 |
| 采集质量 | `qualityreport` | 质量报告 | 报告编号、作业编号、总URL数 |
| 带宽控制 | `bandwidth` | 带宽策略 | 策略编号、节点标识、下载带宽上限 |
| 重试队列 | `retryqueue` | 重试条目 | 条目编号、失败URL、失败原因码 |
| 翻页管理 | `pagemarker` | 翻页规则 | 规则编号、目标站点、翻页类型 |
| 增量标记 | `incrementmark` | 增量水位 | 水位编号、目标源、增量字段名 |
| 配置同步 | `configsync` | 同步任务 | 任务编号、源节点、目标节点 |
| 采集API | `scrapeapi` | API定义 | API编号、API名称、请求路径 |
| 数据抽样 | `datasample` | 抽样方案 | 方案编号、数据源、抽样方式 |
| 采集告警 | `alertpolicy` | 告警策略 | 策略编号、监控指标、触发条件 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 重试队列链路

重试队列按固定链路流转，全程可回溯：

1. **去重重排**（`POST /api/retryqueue/rebuild`）：同一失败URL只保留一个有效排队位置，
   重复条目归并留痕；超过 72h 失败时效的条目归档为最终失败；退避级别决定队列位置，
   调整退避会移动位置且旧位置立即释放。
2. **跨节点派发**（`POST /api/retryqueue/dispatch`）：读取等待条目、节点新鲜度
   （心跳滞后）与最近派发次数，按公平权重 `新鲜度×10/(1+最近派发次数)` 选节点并校验
   并发槽位容量，生成重试进度与节点派发数据，每条进度携带可核对的公平依据。
   并发时只保留一个在途批次；同一幂等键重复提交重放同一批次；负载快照变化后可
   `冲突处置=重排`，旧批次整组标记被覆盖（保留优先级、可回溯）。
3. **终端消费**（`POST /api/retryqueue/consume`）：工作终端侧消费派发结果并累加
   最近派发次数，下一轮可调度结果据此重算。
4. **结果回传**（`POST /api/retryqueue/results`）：最终处理结果先同步到工作终端侧
   清单，再由清单更新节点自身记录，最后重试进度以清单为消费依据推进（成功终态 /
   失败退避重排）。节点离线或任务取消后的迟到结果按幽灵拒绝留痕，不生成新的重试
   进度；同一结果幂等键重复提交直接跳过。

回溯接口：`GET /api/retryqueue/progress`（重试进度）、`/node-dispatch`（节点派发）、
`/worker-results`（终端清单）、`/node-snapshot`（负载快照）、`/locate`（定位条目）。
列表支持按失败原因码筛选、按队列位置/退避级别/首次失败时间排序。

领域：数据工程
子方向：数据采集、爬虫与RPA
