import { history, useLocation, useModel } from '@umijs/max';
import {
  Alert,
  Breadcrumb,
  Button,
  Collapse,
  Form,
  Input,
  Select,
  Tabs,
  Tag,
  Tooltip,
} from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { api, type Operation, type Row } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';
import { DataGrid } from '../../operator/DataGrid';
import { operationLabel } from '../../operator/labels';
import { breadcrumbs, destination, pageFor } from '../../operator/navigation';
import { ResultView } from '../../operator/ResultView';
import { formArguments, SchemaFields } from '../../operator/SchemaFields';

export default function ApiWorkbench() {
  const { catalog, refresh, epoch } = useModel('operator');
  const { pathname, search } = useLocation();
  const [form] = Form.useForm();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [ticket, setTicket] = useState<Row>();
  const [result, setResult] = useState<Row>();
  const [raw, setRaw] = useState('{}');
  const [inputMode, setInputMode] = useState('form');
  const [protocol, setProtocol] = useState('all');
  const [nativeSearch, setNativeSearch] = useState('');
  const developer = pathname === '/settings/developer/api';
  const native = pathname === '/settings/developer/native';
  const page = pageFor(pathname);
  const collection = useMemo(
    () =>
      [
        ...catalog.tools.map((tool: Row) => ({
          ...tool,
          kind: 'mcp',
          key: `mcp:${tool.name}`,
        })),
        ...catalog.routes.map((route: Row) => ({
          ...route,
          kind: 'rest',
          key: `rest:${route.name}`,
        })),
      ].filter(
        (tool) =>
          (developer || destination(tool) === pathname) &&
          (protocol === 'all' || tool.kind === protocol),
      ),
    [catalog, pathname, developer, protocol],
  );
  const selected = new URLSearchParams(search).get('operation');
  const tool =
    collection.find((tool) => tool.key === selected) ||
    collection.find(
      (tool) => tool.annotations?.readOnlyHint || tool.method === 'GET',
    ) ||
    collection[0];
  const readOnly =
    tool?.kind === 'rest'
      ? tool.method === 'GET'
      : tool?.annotations?.readOnlyHint;
  const schema = tool?.inputSchema;
  useEffect(() => {
    form.resetFields();
    const defaults: Row = {};
    const properties = schema?.properties || {};
    for (const [key, field] of Object.entries(properties) as [string, Row][])
      if (field.default !== undefined) defaults[key] = field.default;
    if (properties.tdMode) defaults.tdMode = 'cash';
    form.setFieldsValue(defaults);
    setInputMode(schema ? 'form' : 'json');
    setRaw('{}');
    setResult(undefined);
    setError('');
    setTicket(undefined);
  }, [tool?.key, epoch, form, schema]);
  useEffect(() => {
    setProtocol('all');
  }, [pathname]);
  async function run(values: Row) {
    if (!tool) return;
    setBusy(true);
    setError('');
    setResult(undefined);
    try {
      const operation: Operation = {
        kind: tool.kind,
        name: tool.name,
        arguments:
          inputMode === 'form' && schema
            ? formArguments(values, schema)
            : JSON.parse(raw),
      };
      if (readOnly) setResult(await api('query', operation));
      else setTicket(await api('prepare', operation));
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  const nativeRows = catalog.nativeMethods.filter((row: Row) =>
    `${row.area} ${row.method}`
      .toLowerCase()
      .includes(nativeSearch.toLowerCase()),
  );
  return (
    <div className="management-page business-page">
      <Breadcrumb items={breadcrumbs(pathname).map((title) => ({ title }))} />
      <div className="page-heading">
        <h1>{page?.name || '接口浏览器'}</h1>
      </div>
      {native ? (
        <>
          <div className="function-toolbar">
            <Input.Search
              aria-label="搜索引擎接口"
              placeholder="搜索方法"
              value={nativeSearch}
              onChange={(event) => setNativeSearch(event.target.value)}
            />
            <Tag>源码目录</Tag>
          </div>
          <DataGrid
            rows={nativeRows}
            height={560}
            columns={[
              { key: 'area', title: '模块', width: 270 },
              { key: 'method', title: '方法', width: 290 },
              { key: 'source', title: '源码', width: 480 },
              { key: 'line', title: '行号', width: 80 },
            ]}
          />
        </>
      ) : (
        <>
          <div className="function-toolbar">
            <Select
              aria-label="功能"
              showSearch={{ optionFilterProp: 'label' }}
              value={tool?.key}
              onChange={(value) =>
                history.replace(
                  `${pathname}?operation=${encodeURIComponent(value)}`,
                )
              }
              options={collection.map((item) => ({
                value: item.key,
                label:
                  operationLabel(item) +
                  (developer
                    ? ` · ${item.kind.toUpperCase()}`
                    : item.kind === 'rest'
                      ? ' · API'
                      : ''),
              }))}
              style={{ minWidth: 260, flex: 1 }}
              placeholder="选择功能"
            />
            {developer && (
              <Select
                aria-label="接口类型"
                value={protocol}
                onChange={setProtocol}
                options={[
                  { value: 'all', label: '全部' },
                  { value: 'mcp', label: 'MCP' },
                  { value: 'rest', label: 'REST' },
                ]}
                style={{ width: 120 }}
              />
            )}
          </div>
          {tool ? (
            <section className="panel function-panel">
              <Form
                form={form}
                layout="vertical"
                onFinish={(values) => void run(values)}
              >
                {schema && (
                  <Tabs
                    activeKey={inputMode}
                    onChange={(mode) => {
                      if (mode === 'json') {
                        try {
                          setRaw(
                            JSON.stringify(
                              formArguments(form.getFieldsValue(true), schema),
                              null,
                              2,
                            ),
                          );
                        } catch {
                          setRaw('{}');
                        }
                      }
                      setInputMode(mode);
                    }}
                    items={[
                      { key: 'form', label: '参数' },
                      { key: 'json', label: 'JSON' },
                    ]}
                  />
                )}
                {inputMode === 'form' && schema ? (
                  <SchemaFields schema={schema} />
                ) : (
                  <Form.Item label="参数">
                    <Input.TextArea
                      aria-label="JSON 参数"
                      rows={9}
                      value={raw}
                      onChange={(event) => setRaw(event.target.value)}
                      className="code-input"
                    />
                  </Form.Item>
                )}
                <div className="form-actions">
                  <Tooltip title={tool.blocked}>
                    <span>
                      <Button
                        htmlType="submit"
                        type="primary"
                        loading={busy}
                        disabled={!!tool.blocked}
                      >
                        {readOnly ? '查询' : '提交'}
                      </Button>
                    </span>
                  </Tooltip>
                  {tool.blocked && <Tag>未启用</Tag>}
                </div>
              </Form>
              {error && <Alert type="error" title={error} showIcon />}
              <ResultView value={result} />
              <Collapse
                ghost
                items={[
                  {
                    key: 'details',
                    label: '接口详情',
                    children: (
                      <>
                        <code>{tool.name}</code>
                        {tool.description && (
                          <p className="field-note">{tool.description}</p>
                        )}
                        {tool.source && (
                          <a
                            href={tool.source.replace('/en/', '/zh/')}
                            target="_blank"
                            rel="noreferrer"
                          >
                            文档
                          </a>
                        )}
                        {tool.blocked && <p>{tool.blocked}</p>}
                      </>
                    ),
                  },
                ]}
              />
            </section>
          ) : (
            <div className="empty-state">暂无功能</div>
          )}
        </>
      )}
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(value) => {
          setResult(value);
          void refresh();
        }}
      />
    </div>
  );
}
