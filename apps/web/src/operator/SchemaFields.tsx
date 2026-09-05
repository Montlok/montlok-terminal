import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { Button, Form, Input, InputNumber, Select, Switch } from 'antd';
import type { Row } from './api';
import { fieldLabel, valueLabel } from './labels';

export const BODY = '__request_body';
const commonEnums: Record<string, string[]> = {
  side: ['buy', 'sell'],
  tdMode: ['cash', 'cross', 'isolated', 'spot_isolated'],
  instType: ['SPOT', 'SWAP', 'FUTURES', 'OPTION', 'MARGIN'],
  posSide: ['net', 'long', 'short'],
  tgtCcy: ['base_ccy', 'quote_ccy'],
};

function Field({
  schema,
  name,
  label,
  required = false,
}: {
  schema: Row;
  name: (string | number)[];
  label: string;
  required?: boolean;
}) {
  const key = String(name[name.length - 1]);
  if (schema.type === 'array')
    return (
      <div className="schema-collection">
        <div className="collection-label">{label}</div>
        <Form.List
          name={name}
          rules={
            required
              ? [
                  {
                    validator: async (_, values) => {
                      if (!values?.length) throw new Error(`添加${label}`);
                    },
                  },
                ]
              : []
          }
        >
          {(fields, { add, remove }, { errors }) => (
            <>
              {fields.map((field) => (
                <div key={field.key} className="collection-row">
                  {schema.items?.type === 'object' ? (
                    <SchemaFields schema={schema.items} prefix={[field.name]} />
                  ) : (
                    <Field
                      schema={schema.items || { type: 'string' }}
                      name={[field.name]}
                      label={label}
                      required
                    />
                  )}
                  <Button
                    aria-label={`删除${label}`}
                    icon={<MinusCircleOutlined />}
                    type="text"
                    onClick={() => remove(field.name)}
                  />
                </div>
              ))}
              <Button
                icon={<PlusOutlined />}
                onClick={() =>
                  add(
                    schema.items?.type === 'object' &&
                      Object.keys(schema.items.properties || {}).length
                      ? {}
                      : '',
                  )
                }
              >
                添加{label}
              </Button>
              <Form.ErrorList errors={errors} />
            </>
          )}
        </Form.List>
      </div>
    );
  if (schema.type === 'object' && Object.keys(schema.properties || {}).length)
    return (
      <div className="schema-collection">
        <div className="collection-label">{label}</div>
        <SchemaFields schema={schema} prefix={name} />
      </div>
    );
  const choices = schema.enum || commonEnums[key];
  return (
    <Form.Item
      name={name}
      label={label}
      tooltip={schema.description || key}
      valuePropName={schema.type === 'boolean' ? 'checked' : 'value'}
      rules={[{ required, message: `填写${label}` }]}
    >
      {schema.type === 'object' ? (
        <Input.TextArea rows={3} placeholder="JSON" />
      ) : choices ? (
        <Select
          allowClear
          options={choices.map((value: string) => ({
            value,
            label: valueLabel(value),
          }))}
        />
      ) : schema.type === 'boolean' ? (
        <Switch />
      ) : schema.type === 'integer' || schema.type === 'number' ? (
        <InputNumber style={{ width: '100%' }} />
      ) : (
        <Input autoComplete="off" />
      )}
    </Form.Item>
  );
}

export function SchemaFields({
  schema,
  prefix = [],
}: {
  schema: Row;
  prefix?: (string | number)[];
}) {
  if (schema.type === 'array')
    return (
      <Field schema={schema} name={[...prefix, BODY]} label="订单" required />
    );
  if (!Object.keys(schema.properties || {}).length && prefix.length)
    return (
      <Field schema={{ type: 'object' }} name={prefix} label="参数" required />
    );
  return (
    <div className="schema-fields">
      {Object.entries(schema.properties || {})
        .filter(([key]) => key !== 'demo')
        .map(([key, field]) => (
          <Field
            key={key}
            schema={field as Row}
            name={[...prefix, key]}
            label={fieldLabel(key, field as Row)}
            required={schema.required?.includes(key)}
          />
        ))}
    </div>
  );
}

export function formArguments(values: Row, schema: Row): Row | Row[] {
  function convert(value: any, node: Row): any {
    if (value === undefined || value === '') return undefined;
    if (node.type === 'array')
      return (value || []).map((item: any) => convert(item, node.items || {}));
    if (node.type === 'object') {
      if (typeof value === 'string') return JSON.parse(value);
      const result: Row = {};
      for (const [key, field] of Object.entries(node.properties || {})) {
        if (key === 'demo') continue;
        const converted = convert(value?.[key], field as Row);
        if (converted !== undefined) result[key] = converted;
      }
      return Object.keys(node.properties || {}).length ? result : value;
    }
    return value;
  }
  return convert(schema.type === 'array' ? values[BODY] : values, schema) || {};
}
