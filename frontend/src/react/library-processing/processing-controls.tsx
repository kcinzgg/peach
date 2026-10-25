import { Button } from '@/components/base/buttons/button';
import { Input } from '@/components/base/input/input';
import { useId, useState } from 'react';
import { RiPauseLine, RiPlayLine } from '@remixicon/react';

import { apiSend } from '../../api';
import { ProcessingMode } from './processing-effort';
import { queryClient } from '../query';
import { busyProps, useAction } from '../settings/use-action';
import {
  LIBRARY_PROCESSING_KEY, LIBRARY_PROCESSING_URL, processingActive,
  type LibraryProcessingData, type ProcessingControlCommand,
} from './library-processing';

/** 控制只作用于快照里的任务号；状态由服务端确认后交给所有入口。 */
export function ProcessingControls({ state, disabled = false, showConcurrency = false }: {
  state: LibraryProcessingData; disabled?: boolean; showConcurrency?: boolean;
}) {
  const action = useAction();
  const active = processingActive(state);
  const controlLabel = state.status === 'paused' ? '继续' : state.status === 'pausing' ? '正在暂停' : '暂停';
  const change = (command: ProcessingControlCommand) => {
    if (disabled || action.busy) return;
    void action.run(command.action, signal =>
      apiSend<LibraryProcessingData>(LIBRARY_PROCESSING_URL, command, 'POST', signal), next => {
        queryClient.setQueryData(LIBRARY_PROCESSING_KEY, next);
        void queryClient.invalidateQueries({ queryKey: LIBRARY_PROCESSING_KEY });
      });
  };
  return <div className="flex min-w-0 flex-col gap-2" data-processing-controls="">
    <div className="flex flex-wrap items-center gap-2">
      <ProcessingMode value={state.speed_mode || 'standard'} busy={disabled || !!action.busy}
        animate={state.status !== 'paused' && state.status !== 'pausing'}
        onChange={speed_mode => change({ action: 'set_speed', speed_mode, ...(active ? { job_id: state.job_id } : {}) })} />
      {active ? <Button size="small" variant="secondary" iconOnly
        leadingIcon={state.status === 'paused' ? RiPlayLine : RiPauseLine}
        aria-label={controlLabel} title={controlLabel}
        disabled={disabled || !state.job_id || state.status === 'pausing'}
        {...busyProps(action.busy === 'pause' || action.busy === 'resume')}
        onClick={() => {
          if (state.job_id) change({ action: state.status === 'paused' ? 'resume' : 'pause', job_id: state.job_id });
        }} /> : null}
    </div>
    {showConcurrency ? <ConcurrencyControl key={state.metadata_concurrency ?? 6}
      value={state.metadata_concurrency ?? 6} disabled={disabled || !!action.busy}
      apply={metadata_concurrency => change({ action: 'set_concurrency', metadata_concurrency,
        ...(active ? { job_id: state.job_id } : {}) })} /> : null}
    {action.error ? <p role="alert" className="text-caption-1-regular text-text-error-primary">{action.error}</p> : null}
  </div>;
}

function ConcurrencyControl({ value, disabled, apply }: { value: number; disabled: boolean; apply(value: number): void }) {
  const [draft, setDraft] = useState(String(value));
  const hintId = useId();
  const valid = /^\d+$/.test(draft) && Number(draft) >= 1 && Number(draft) <= 100;
  return <div className="flex min-w-0 flex-col gap-2" data-processing-concurrency="">
    <div className="flex flex-wrap items-end gap-2">
      <div className="min-w-0 flex-1">
        <Input label="资料采集并发数" size="small" type="number" inputMode="numeric" autoComplete="off"
          value={draft} onChange={setDraft} isDisabled={disabled} isInvalid={!valid} aria-describedby={hintId} />
      </div>
      <Button size="small" variant="secondary" disabled={disabled || !valid || Number(draft) === value}
        onClick={() => apply(Number(draft))}>应用并发数</Button>
    </div>
    <p id={hintId} className="text-caption-1-regular text-text-secondary" aria-live="polite">
      {!valid ? '请输入 1–100 的整数' : '默认 6，最多 100；各网站独立限流。'}
    </p>
    <p className="text-caption-1-regular text-text-secondary">运行中可调整；降低并发后，在途任务完成再补位。</p>
  </div>;
}
