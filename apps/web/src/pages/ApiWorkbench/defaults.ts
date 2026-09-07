import type { Row } from '../../operator/api';

export function operationDefaults(schema: Row | undefined, path: string): Row {
  const properties = schema?.properties || {};
  const values: Row = {};
  for (const [key, field] of Object.entries(properties) as [string, Row][])
    if (field.default !== undefined) values[key] = field.default;
  const product = path.includes('/swap/')
    ? 'SWAP'
    : path.includes('/futures/')
      ? 'FUTURES'
      : path.includes('/option/')
        ? 'OPTION'
        : path.includes('/spot/')
          ? 'SPOT'
          : undefined;
  if (product && properties.instType && values.instType === undefined)
    values.instType = product;
  if (product && properties.tdMode && values.tdMode === undefined)
    values.tdMode = product === 'SPOT' ? 'cash' : 'cross';
  return values;
}

export function canAutoQuery(schema: Row | undefined, values: Row): boolean {
  return (
    !!schema &&
    schema.type !== 'array' &&
    (schema.required || []).every(
      (key: string) =>
        values[key] !== undefined && values[key] !== '' && values[key] !== null,
    )
  );
}
