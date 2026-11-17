import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/retryqueue'
const WORKER_ENDPOINT = '/api/worker'

const columns = ['条目编号', '失败URL', '失败原因码', '退避级别', '队列位置', '已重试次数', '下次重试时间']
const statuses = ['草稿', '等待重试', '重试中', '重试成功', '最终失败', '已归并', '已取消']
const sortFields = ['队列位置', '退避级别', '首次失败时间', '已重试次数', '条目编号']
const tabs = ['唯一队列', '重试进度', '节点派发', '终端清单', '节点快照'] as const

type Row = Record<string, any>
type Tab = (typeof tabs)[number]

function badgeClass(value: unknown): string {
  const text = String(value ?? '')
  if (['重试成功', '已接受', '可调度', '已消费'].includes(text)) return 'badge ok'
  if (['最终失败', '已取消', '幽灵拒绝', '被覆盖', '离线'].includes(text)) return 'badge err'
  if (['等待重试', '重试中', '待消费', '已派发', '重新排队'].includes(text)) return 'badge warn'
  return 'badge'
}

export default function RetryqueueView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [stats, setStats] = useState<Row>({})
  const [queueItems, setQueueItems] = useState<Row[]>([])
  const [progressItems, setProgressItems] = useState<Row[]>([])
  const [dispatchItems, setDispatchItems] = useState<Row[]>([])
  const [resultItems, setResultItems] = useState<Row[]>([])
  const [snapshot, setSnapshot] = useState<Row>({ 节点: [] })
  const [activeTab, setActiveTab] = useState<Tab>('唯一队列')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [locateUrl, setLocateUrl] = useState('')
  const [highlightCode, setHighlightCode] = useState('')
  const [idemKey, setIdemKey] = useState(() => `IDEM-${Math.random().toString(36).slice(2, 8).toUpperCase()}`)
  const [createOpen, setCreateOpen] = useState(false)
  const [createForm, setCreateForm] = useState<Record<string, string>>({ 条目编号: '', 失败URL: '', 失败原因码: 'HTTP_500' })
  const [notice, setNotice] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const newIdemKey = () => `IDEM-${Math.random().toString(36).slice(2, 8).toUpperCase()}`

  const reload = useCallback(async () => {
    const query = new URLSearchParams()
    if (filters.keyword) query.set('keyword', filters.keyword)
    if (filters.失败原因码) query.set('reason', filters.失败原因码)
    if (filters.status) query.set('status', filters.status)
    if (filters.sort) query.set('sort', filters.sort)
    if (filters.order) query.set('order', filters.order)
    const [listResp, statsResp, queueResp, progressResp, dispatchResp, resultsResp, snapshotResp] = await Promise.all([
      request(`${ENDPOINT}?${query}`),
      request(`${ENDPOINT}/stats`),
      request(`${ENDPOINT}/queue`),
      request(`${ENDPOINT}/progress`),
      request(`${ENDPOINT}/node-dispatch`),
      request(`${ENDPOINT}/worker-results`),
      request(`${ENDPOINT}/node-snapshot`),
    ])
    if (!listResp.ok) throw new Error('重试条目列表读取失败')
    const payload = await listResp.json()
    setRows(payload.items ?? [])
    setTotal(payload.total ?? 0)
    setStats(await statsResp.json())
    setQueueItems((await queueResp.json()).items ?? [])
    setProgressItems((await progressResp.json()).items ?? [])
    setDispatchItems((await dispatchResp.json()).items ?? [])
    setResultItems((await resultsResp.json()).items ?? [])
    setSnapshot(await snapshotResp.json())
  }, [filters])

  const refresh = useCallback(async () => {
    setErrorMessage('')
    try {
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '重试队列数据读取失败')
    }
  }, [reload])

  useEffect(() => {
    void refresh()
  }, [refresh])

  async function post(path: string, values: Record<string, unknown> = {}): Promise<Row | null> {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(path, { method: 'POST', body: JSON.stringify({ values }) })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        setErrorMessage(payload.message ?? payload.detail ?? '操作未生效')
        await reload()
        return null
      }
      setNotice(payload.message ?? '操作完成')
      await reload()
      return payload
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '操作失败')
      return null
    }
  }

  async function runAction(row: Row, action: string) {
    await post(`${ENDPOINT}/${row.id}/actions`, { action })
  }

  async function changeBackoff(row: Row, delta: number) {
    await post(`${ENDPOINT}/${row.id}/backoff`, { 调整: delta })
  }

  async function doDispatch(conflict: string) {
    const payload = await post(`${ENDPOINT}/dispatch`, { 幂等键: idemKey, 冲突处置: conflict })
    if (payload?.ok && !payload.replayed) {
      setActiveTab('重试进度')
    }
  }

  async function doConsume() {
    const payload = await post(`${ENDPOINT}/consume`)
    if (payload?.ok) {
      setIdemKey(newIdemKey())
      setActiveTab('节点派发')
    }
  }

  async function doAutoResults() {
    const payload = await post(`${ENDPOINT}/results`)
    if (payload?.ok) setActiveTab('终端清单')
  }

  async function doResultOne(progressNo: string, outcome: string) {
    const payload = await post(`${ENDPOINT}/results`, { results: [{ 进度编号: progressNo, 处理结果: outcome }] })
    if (payload?.ok) setActiveTab('终端清单')
  }

  async function doLocate() {
    setErrorMessage('')
    setNotice('')
    if (!locateUrl.trim()) return
    try {
      const response = await request(`${ENDPOINT}/locate?url=${encodeURIComponent(locateUrl.trim())}`)
      const payload = await response.json()
      if (!response.ok) {
        setErrorMessage(payload.detail ?? '未找到匹配的重试条目')
        setHighlightCode('')
        return
      }
      const code = payload.条目?.条目编号 ?? ''
      setHighlightCode(code)
      setNotice(
        payload.队列位置
          ? `已定位 ${code}：唯一队列位置 ${payload.队列位置}/${payload.队列长度}（${payload.位置依据}）`
          : `已定位 ${code}：当前不持有队列位置（${payload.条目?.status}）`,
      )
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '定位失败')
    }
  }

  async function doCreate() {
    const payload = await post(ENDPOINT, createForm)
    if (payload?.ok) {
      setCreateOpen(false)
      setCreateForm({ 条目编号: '', 失败URL: '', 失败原因码: 'HTTP_500' })
    }
  }

  async function workerAction(node: Row, action: string) {
    await post(`${WORKER_ENDPOINT}/${node.id}/actions`, { action })
  }

  const statCards = [
    { label: '重试条目总数', value: stats.重试条目总数 ?? 0 },
    { label: '等待重试数', value: stats.等待重试数 ?? 0 },
    { label: '重试成功率', value: stats.重试成功率 ?? '—' },
    { label: '幽灵拒绝数', value: stats.幽灵拒绝数 ?? 0 },
    { label: '在途批次', value: stats.在途批次 ?? '无' },
    { label: '负载快照', value: `v${stats.负载快照版本 ?? 1}` },
  ]

  return (
    <section className="page" data-module="retryqueue">
      <header className="page-head">
        <div>
          <h2>重试队列管理</h2>
          <p className="page-desc">
            失败URL去重与跨节点派发：去重重排 → 生成派发计划（幂等）→ 终端消费 → 结果回传落清单 → 进度消费，全链路可回溯。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setCreateOpen((prev) => !prev)}>
            登记重试条目
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出重试队列清单
          </button>
        </div>
      </header>

      {createOpen && (
        <form
          className="filter-bar create-form"
          onSubmit={(event) => {
            event.preventDefault()
            void doCreate()
          }}
        >
          <label className="filter-item">
            <span>条目编号</span>
            <input
              value={createForm.条目编号}
              placeholder="如 RETR-0009"
              onChange={(event) => setCreateForm((prev) => ({ ...prev, 条目编号: event.target.value }))}
            />
          </label>
          <label className="filter-item">
            <span>失败URL</span>
            <input
              value={createForm.失败URL}
              placeholder="https://example.com/page"
              onChange={(event) => setCreateForm((prev) => ({ ...prev, 失败URL: event.target.value }))}
            />
          </label>
          <label className="filter-item">
            <span>失败原因码</span>
            <input
              value={createForm.失败原因码}
              onChange={(event) => setCreateForm((prev) => ({ ...prev, 失败原因码: event.target.value }))}
            />
          </label>
          <button className="btn primary" type="submit">提交登记</button>
        </form>
      )}

      <div className="stat-row">
        {statCards.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <div className="chain-bar">
        <button className="btn" type="button" onClick={() => void post(`${ENDPOINT}/rebuild`)}>去重重排</button>
        <button className="btn primary" type="button" onClick={() => void doDispatch('拒绝')}>生成派发计划</button>
        <button className="btn" type="button" onClick={() => void doDispatch('重排')}>冲突重排（保留优先级）</button>
        <button className="btn" type="button" onClick={() => void doConsume()}>终端消费派发</button>
        <button className="btn" type="button" onClick={() => void doAutoResults()}>回传全部在途结果</button>
        <label className="filter-item">
          <span>幂等键</span>
          <input className="idem-input" value={idemKey} onChange={(event) => setIdemKey(event.target.value)} />
        </label>
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void refresh()
        }}
      >
        <label className="filter-item">
          <span>关键字</span>
          <input
            value={filters.keyword ?? ''}
            placeholder="条目编号 / 失败URL"
            onChange={(event) => setFilters((prev) => ({ ...prev, keyword: event.target.value }))}
          />
        </label>
        <label className="filter-item">
          <span>失败原因码</span>
          <input
            value={filters.失败原因码 ?? ''}
            placeholder="如 HTTP_429"
            onChange={(event) => setFilters((prev) => ({ ...prev, 失败原因码: event.target.value }))}
          />
        </label>
        <label className="filter-item">
          <span>状态</span>
          <select value={filters.status ?? ''} onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}>
            <option value="">全部</option>
            {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>排序字段</span>
          <select value={filters.sort ?? ''} onChange={(event) => setFilters((prev) => ({ ...prev, sort: event.target.value }))}>
            <option value="">默认</option>
            {sortFields.map((field) => <option key={field} value={field}>{field}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>方向</span>
          <select value={filters.order ?? 'asc'} onChange={(event) => setFilters((prev) => ({ ...prev, order: event.target.value }))}>
            <option value="asc">升序</option>
            <option value="desc">降序</option>
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
        <label className="filter-item">
          <span>定位条目</span>
          <input
            value={locateUrl}
            placeholder="输入失败URL定位队列位置"
            onChange={(event) => setLocateUrl(event.target.value)}
          />
        </label>
        <button className="btn" type="button" onClick={() => void doLocate()}>定位</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>状态</th>
            <th>版本</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)} className={row.条目编号 === highlightCode ? 'highlight' : ''}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td><span className={badgeClass(row.status)}>{row.status}</span></td>
              <td>{row.版本 ?? '—'}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void runAction(row, '加入队列')}>加入队列</button>
                <button className="link" type="button" onClick={() => void runAction(row, '发起重试')}>发起重试</button>
                <button className="link" type="button" onClick={() => void changeBackoff(row, 1)}>退避+1</button>
                <button className="link" type="button" onClick={() => void changeBackoff(row, -1)}>退避-1</button>
                <button className="link" type="button" onClick={() => void runAction(row, '取消任务')}>取消任务</button>
                <button className="link" type="button" onClick={() => void runAction(row, '标记最终失败')}>标记最终失败</button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 3}>暂无重试队列数据，可先登记重试条目</td>
            </tr>
          )}
        </tbody>
      </table>

      <div className="tabs">
        {tabs.map((tab) => (
          <button key={tab} className={`tab${activeTab === tab ? ' active' : ''}`} type="button" onClick={() => setActiveTab(tab)}>
            {tab}
          </button>
        ))}
      </div>

      {activeTab === '唯一队列' && (
        <table className="data-table">
          <thead>
            <tr><th>队列位置</th><th>条目编号</th><th>失败URL</th><th>退避级别</th><th>位置依据（可核对）</th></tr>
          </thead>
          <tbody>
            {queueItems.map((item) => (
              <tr key={item.条目编号} className={item.条目编号 === highlightCode ? 'highlight' : ''}>
                <td>{item.队列位置}</td>
                <td>{item.条目编号}</td>
                <td>{item.失败URL}</td>
                <td>{item.退避级别}</td>
                <td><span className="reason">{item.位置依据}</span></td>
              </tr>
            ))}
            {queueItems.length === 0 && <tr><td className="empty-state" colSpan={5}>唯一队列为空</td></tr>}
          </tbody>
        </table>
      )}

      {activeTab === '重试进度' && (
        <table className="data-table">
          <thead>
            <tr>
              <th>进度编号</th><th>批次号</th><th>条目编号</th><th>目标节点</th><th>快照版本</th>
              <th>状态</th><th>公平依据 / 覆盖原因</th><th>生成时间</th><th>回传</th>
            </tr>
          </thead>
          <tbody>
            {progressItems.map((item) => (
              <tr key={item.进度编号}>
                <td>{item.进度编号}</td>
                <td>{item.批次号}</td>
                <td>{item.条目编号}</td>
                <td>{item.目标节点}</td>
                <td>v{item.负载快照版本}·第{item.进度版本}版</td>
                <td><span className={badgeClass(item.状态)}>{item.状态}</span></td>
                <td>
                  <ul className="reason">
                    {(item.公平依据 ?? []).map((line: string) => <li key={line}>{line}</li>)}
                    {item.覆盖原因 && <li>{item.覆盖原因}</li>}
                  </ul>
                </td>
                <td>{item.生成时间}</td>
                <td className="row-actions">
                  {item.状态 === '已派发' && (
                    <>
                      <button className="link" type="button" onClick={() => void doResultOne(item.进度编号, '成功')}>回传成功</button>
                      <button className="link" type="button" onClick={() => void doResultOne(item.进度编号, '失败')}>回传失败</button>
                    </>
                  )}
                </td>
              </tr>
            ))}
            {progressItems.length === 0 && <tr><td className="empty-state" colSpan={9}>暂无重试进度，请先生成派发计划</td></tr>}
          </tbody>
        </table>
      )}

      {activeTab === '节点派发' && (
        <table className="data-table">
          <thead>
            <tr>
              <th>派发编号</th><th>批次号</th><th>节点编号</th><th>派发条数</th><th>容量校验</th>
              <th>快照最近派发</th><th>消费后最近派发</th><th>状态</th>
            </tr>
          </thead>
          <tbody>
            {dispatchItems.map((item) => (
              <tr key={item.派发编号}>
                <td>{item.派发编号}</td>
                <td>{item.批次号}</td>
                <td>{item.节点编号}</td>
                <td>{item.派发条数}</td>
                <td><span className="reason">{item.容量校验}</span></td>
                <td>{item.快照最近派发次数}</td>
                <td>{item.消费后最近派发次数 ?? '—'}</td>
                <td><span className={badgeClass(item.状态)}>{item.状态}</span></td>
              </tr>
            ))}
            {dispatchItems.length === 0 && <tr><td className="empty-state" colSpan={8}>暂无节点派发数据</td></tr>}
          </tbody>
        </table>
      )}

      {activeTab === '终端清单' && (
        <table className="data-table">
          <thead>
            <tr>
              <th>结果编号</th><th>进度编号</th><th>条目编号</th><th>节点编号</th><th>处理结果</th>
              <th>处置</th><th>拒绝原因</th><th>接收时间</th>
            </tr>
          </thead>
          <tbody>
            {resultItems.map((item) => (
              <tr key={item.结果编号}>
                <td>{item.结果编号}</td>
                <td>{item.进度编号}</td>
                <td>{item.条目编号}</td>
                <td>{item.节点编号}</td>
                <td>{item.处理结果}</td>
                <td><span className={badgeClass(item.处置)}>{item.处置}</span></td>
                <td><span className="reason">{item.拒绝原因 || '—'}</span></td>
                <td>{item.接收时间}</td>
              </tr>
            ))}
            {resultItems.length === 0 && <tr><td className="empty-state" colSpan={8}>工作终端侧清单为空，等待终端回传</td></tr>}
          </tbody>
        </table>
      )}

      {activeTab === '节点快照' && (
        <table className="data-table">
          <thead>
            <tr>
              <th>节点编号</th><th>在线</th><th>新鲜度</th><th>剩余容量</th><th>最近派发次数</th>
              <th>公平权重</th><th>可调度</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(snapshot.节点 ?? []).map((node: Row) => (
              <tr key={node.节点编号}>
                <td>{node.节点编号}</td>
                <td>{node.是否在线 ? '在线' : '离线'}</td>
                <td>{node.新鲜度}（{node.新鲜度说明}）</td>
                <td>{node.剩余容量}/{node.并发槽位数}</td>
                <td>{node.最近派发次数}</td>
                <td>{node.公平权重}</td>
                <td><span className={badgeClass(node.可调度 ? '可调度' : '离线')}>{node.可调度 ? '可调度' : node.不可调度原因}</span></td>
                <td className="row-actions">
                  <button className="link" type="button" onClick={() => void workerAction(node, '标记在线')}>标记在线</button>
                  <button className="link" type="button" onClick={() => void workerAction(node, '标记离线')}>标记离线</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <footer className="page-foot">
        <span>共 {total} 条重试队列记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
