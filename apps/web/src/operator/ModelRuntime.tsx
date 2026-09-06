import { number, type Row, timeOf } from './api';
import { modelRuntimeCharts } from './modelRuntimeCharts';
import { TimeSeriesChart } from './TimeSeriesChart';

export function ModelRuntime({ model }: { model?: Row }) {
  if (!model) return <p className="run-detail">暂无模型运行数据</p>;
  const window = model.budgetWindow || model.window;
  const charts = modelRuntimeCharts(model.recentInferences);
  return (
    <>
      <dl className="run-facts" aria-label="模型推理状态">
        <div>
          <dt>模型编号</dt>
          <dd>{model.modelId || model.releaseId || '未上报'}</dd>
        </div>
        <div>
          <dt>运行版本</dt>
          <dd>{model.modelVersion || model.version || '未上报'}</dd>
        </div>
        <div>
          <dt>推理设备</dt>
          <dd>
            {model.device || '未上报'}
            {model.warmupComplete === false ? '（模型尚未就绪）' : ''}
          </dd>
        </div>
        <div>
          <dt>最近推理</dt>
          <dd>
            {model.lastInferenceAt
              ? timeOf(model.lastInferenceAt)
              : '尚无推理记录'}
          </dd>
        </div>
        <div>
          <dt>推理耗时</dt>
          <dd>
            {model.latencyMs == null
              ? '未上报'
              : `${number(model.latencyMs, 3)} ms`}
          </dd>
        </div>
        <div>
          <dt>排队耗时</dt>
          <dd>
            {model.queueMs == null
              ? '未上报'
              : `${number(model.queueMs, 3)} ms`}
          </dd>
        </div>
        <div>
          <dt>特征窗口</dt>
          <dd>
            {model.windowReady === true
              ? '已就绪'
              : model.windowReady === false
                ? '尚未就绪'
                : '未上报'}
          </dd>
        </div>
        <div>
          <dt>推理预算窗口</dt>
          <dd>
            {window
              ? `${window.usedMs == null ? '未上报' : number(window.usedMs, 3)} / ${window.limitMs == null ? '未上报' : number(window.limitMs, 3)} ms`
              : '未上报'}
          </dd>
        </div>
        {model.status && (
          <div>
            <dt>推理状态</dt>
            <dd>
              {(
                {
                  ready: '已就绪',
                  warming: '准备窗口中',
                  running: '推理中',
                  failed: '推理失败',
                  unavailable: '不可用',
                  active: '运行中',
                  waiting_for_features: '等待特征窗口',
                  error: '推理异常',
                  stopped: '已停止',
                } as Record<string, string>
              )[model.status] || model.status}
            </dd>
          </div>
        )}
        {model.lastInferenceError && (
          <div>
            <dt>最近推理异常</dt>
            <dd>{model.lastInferenceError}</dd>
          </div>
        )}
        <div>
          <dt>技术记录</dt>
          <dd>
            <details>
              <summary>完整推理记录</summary>
              <pre className="json-output">
                {JSON.stringify(model, null, 2)}
              </pre>
            </details>
          </dd>
        </div>
      </dl>
      {Array.isArray(model.recentInferences) && (
        <section className="run-section" aria-label="推理观测">
          <strong>预测收益 / bps</strong>
          {charts.predictions.length > 1 ? (
            <TimeSeriesChart
              label="模型预测收益 bps"
              points={charts.predictions}
              height={120}
            />
          ) : (
            <p className="run-detail">
              {charts.predictions.length ? '已记录 1 次预测' : '尚无预测观测'}
            </p>
          )}
          <strong>目标资金比例 / %</strong>
          {charts.targets.length > 1 ? (
            <TimeSeriesChart
              label="模型目标资金比例 %"
              points={charts.targets}
              height={120}
              color="#b99cff"
            />
          ) : (
            <p className="run-detail">
              {charts.targets.length
                ? '已记录 1 次目标比例'
                : '尚无目标比例观测'}
            </p>
          )}
          <p className="run-detail">目标资金比例由模型信号计算。</p>
        </section>
      )}
    </>
  );
}
