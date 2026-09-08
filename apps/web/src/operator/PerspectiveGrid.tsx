import { useEffect, useRef, useState } from 'react';
import type { Client, ColumnType, Table } from '@perspective-dev/client';
import type { HTMLPerspectiveViewerElement } from '@perspective-dev/viewer';
import '@perspective-dev/viewer/themes/pro-dark.css';

let runtime: Promise<Client> | undefined;
const loadModule = (url: string): Promise<unknown> =>
  import(/* webpackIgnore: true */ url);
function engine(): Promise<Client> {
  runtime ??= (async () => {
    const base = '/vendor/perspective-5.3.1/';
    const [clientModule] = await Promise.all([
      loadModule(base + 'cdn/perspective.js'),
      loadModule(base + 'cdn/perspective-viewer.js'),
    ]);
    const perspective = (
      clientModule as typeof import('@perspective-dev/client')
    ).default;
    const clientWasm: ArrayBuffer = await fetch(
      base + 'wasm/perspective-js.wasm',
    ).then((response) => response.arrayBuffer());
    perspective.init_client(clientWasm);
    perspective.init_server(() => fetch(base + 'wasm/perspective-server.wasm'));
    await loadModule(base + 'cdn/perspective-viewer-datagrid.js');
    return perspective.worker();
  })().catch((error) => {
    runtime = undefined;
    throw error;
  });
  return runtime;
}

export type PerspectiveRow = Record<string, string | number | boolean | null>;
/** Explicit schema preserves source precision; financial strings are never coerced to floats. */
export default function PerspectiveGrid({
  rows,
  schema,
  height = 360,
  onSelect,
}: {
  rows: PerspectiveRow[];
  schema: Record<string, ColumnType>;
  height?: number;
  onSelect?: (row: PerspectiveRow) => void;
}) {
  const element = useRef<HTMLPerspectiveViewerElement>(null);
  const table = useRef<Table | undefined>(undefined);
  const pending = useRef(rows);
  const busy = useRef(false);
  const active = useRef(true);
  const [error, setError] = useState('');
  const [ready, setReady] = useState(false);
  const flush = async () => {
    if (busy.current || !table.current || !active.current) return;
    busy.current = true;
    try {
      let current: PerspectiveRow[];
      do {
        current = pending.current;
        await table.current.replace(current);
      } while (active.current && current !== pending.current);
    } catch (reason) {
      if (active.current)
        setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      busy.current = false;
    }
  };
  pending.current = rows;
  const schemaKey = JSON.stringify(schema);
  useEffect(() => {
    active.current = true;
    let disposed = false;
    const viewer = element.current;
    void engine()
      .then(async (client) => {
        const created = await client.table(schema);
        if (disposed) {
          await created.delete();
          return;
        }
        table.current = created;
        await created.replace(pending.current);
        if (disposed) return;
        await viewer?.load(created);
        await viewer?.restore({
          plugin: 'Datagrid',
          theme: 'Pro Dark',
          settings: false,
        });
        if (!disposed) {
          setReady(true);
          void flush();
        }
      })
      .catch((reason) => {
        if (!disposed)
          setError(reason instanceof Error ? reason.message : String(reason));
      });
    return () => {
      disposed = true;
      active.current = false;
      const previous = table.current;
      table.current = undefined;
      void viewer
        ?.delete()
        .catch(() => {})
        .finally(() => previous?.delete())
        .catch(() => {});
    };
  }, [schemaKey]);
  useEffect(() => {
    void flush();
  }, [rows, ready]);
  useEffect(() => {
    const viewer = element.current;
    const listener = (event: Event) => {
      const row = (event as CustomEvent<{ row: PerspectiveRow }>).detail?.row;
      if (row) onSelect?.(row);
    };
    viewer?.addEventListener('perspective-click', listener);
    return () => viewer?.removeEventListener('perspective-click', listener);
  }, [onSelect]);
  return (
    <div
      style={{ height, position: 'relative' }}
      className="perspective-detail"
    >
      {error && (
        <div role="alert" className="execution-note">
          {error}
        </div>
      )}
      {!ready && !error && (
        <div role="status" className="workspace-loading">
          加载明细表格
        </div>
      )}
      <perspective-viewer
        key={schemaKey}
        ref={element}
        style={{
          height: '100%',
          width: '100%',
          display: ready ? 'block' : 'none',
        }}
      />
    </div>
  );
}
