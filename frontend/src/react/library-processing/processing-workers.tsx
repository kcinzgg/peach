import { Disclosure } from '../settings/section';
import { useOverlayScrollbar } from '../components/overlay-scrollbar';
import { processingActive, type LibraryProcessingData } from './library-processing';

const actions: Record<string, string> = {
  querying_metadata: '查询资料', fetching_cover: '采集封面', writing_candidates: '保存资料',
};
const network: Record<string, string> = {
  waiting_source: '等待来源限流', requesting_source: '正在请求', processing: '解析与整理',
};

export function ProcessingWorkers({ state }: { state: LibraryProcessingData }) {
  const list = useOverlayScrollbar<HTMLUListElement>();
  const parallel = state.parallel;
  if (!processingActive(state) || !parallel) return null;
  return <div className="flex min-w-0 flex-col gap-2 text-caption-1-regular text-text-secondary" data-processing-workers="">
    <p className="tabular-nums">处理中 {parallel.active} 项 · 等待来源 {parallel.waiting} 项 · 待处理 {parallel.pending} 项</p>
    <p className="tabular-nums">本轮已处理 {parallel.finished} 项，其中 {parallel.failed} 项有未取得结果；本任务累计补入 {state.auto_applied || 0} 个资料字段、{state.covers || 0} 张封面。</p>
    <Disclosure summary={`并发详情（${parallel.active} 项）`}>
      <p>同一来源最多同时请求 2 次；网站间隔、冷却和重试规则始终生效。</p>
      {parallel.items.length ? <div className="relative">
        <ul ref={list} className="flex max-h-64 min-w-0 flex-col gap-3 overflow-y-auto overscroll-contain pr-3" aria-label="正在处理的影片">
          {parallel.items.map(item => <li key={item.asset_id} className="min-w-0 wrap-anywhere" data-worker-asset={item.asset_id}>
            <a href={`/assets/${item.asset_id}`} className="text-text-primary hover:underline">{item.code || item.name}</a>
            <p>{item.code ? `${item.name} · ` : ''}{actions[item.action] || '处理资料'} · {network[item.network_status] || '处理中'}{item.source ? ` · ${item.source}` : ''}</p>
          </li>)}
        </ul>
      </div> : <p>当前没有在途影片。</p>}
      {parallel.results.length ? <>
        <p className="text-text-primary">最近处理结果</p>
        <ul className="flex min-w-0 flex-col gap-1" aria-label="最近处理结果">
          {parallel.results.map(item => <li key={item.asset_id} className="wrap-anywhere">
            {item.code || item.name} · {item.failed ? '部分结果未取得' : '已处理'}{item.covers ? ` · 封面 +${item.covers}` : ''}
          </li>)}
        </ul>
      </> : null}
    </Disclosure>
  </div>;
}
