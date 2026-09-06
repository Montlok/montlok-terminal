import { EditOutlined, LinkOutlined, PlusOutlined } from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import { useModel } from '@umijs/max';
import { Alert, Button, Form, Input, Select } from 'antd';
import { useState } from 'react';
import { api, type Row } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';

export default function Connections() {
  const { profiles, refresh } = useModel('operator');
  const { initialState } = useModel('@@initialState');
  const canOperate = initialState?.currentUser?.access === 'admin';
  const [form] = Form.useForm();
  const [editing, setEditing] = useState(false);
  const [isNew, setIsNew] = useState(true);
  const [verification, setVerification] = useState<Row>();
  const [ticket, setTicket] = useState<Row>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [receipt, setReceipt] = useState<Row>();
  function edit(profile?: Row) {
    form.resetFields();
    setIsNew(!profile);
    setEditing(true);
    setError('');
    setVerification(undefined);
    form.setFieldsValue(
      profile
        ? { ...profile, apiKey: '', secret: '', passphrase: '' }
        : { mode: 'demo', site: 'global' },
    );
  }
  async function test() {
    setBusy(true);
    setError('');
    try {
      const values = await form.validateFields();
      setVerification(await api('profiles/test', values));
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  async function prepare(name: string, argumentsValue: Row) {
    setError('');
    setBusy(true);
    try {
      setTicket(
        await api('prepare', {
          kind: 'profile',
          name,
          arguments: argumentsValue,
        }),
      );
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>API 连接</h1>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => edit()}
          disabled={!canOperate}
        >
          添加
        </Button>
      </div>
      {error && <Alert type="error" title={error} showIcon />}
      {receipt && (
        <Alert
          type={receipt.status === 'completed' ? 'success' : 'warning'}
          title={receipt.status === 'completed' ? '已保存' : '查看操作结果'}
          description={
            receipt.status === 'completed'
              ? undefined
              : JSON.stringify(receipt.result)
          }
        />
      )}
      <ProTable<Row>
        rowKey="id"
        dataSource={profiles}
        search={false}
        options={false}
        pagination={false}
        size="small"
        className="panel"
        columns={[
          { title: '名称', dataIndex: 'name' },
          {
            title: '环境',
            dataIndex: 'mode',
            render: (_, row) => (row.mode === 'demo' ? 'OKX Demo' : '实盘只读'),
          },
          { title: '站点', dataIndex: 'site' },
          { title: 'API Key', dataIndex: 'keyMask' },
          {
            title: '状态',
            dataIndex: 'active',
            render: (_, row) => (
              <span className={row.active ? 'positive' : 'muted'}>
                {row.active ? '当前使用' : '未启用'}
              </span>
            ),
          },
          {
            title: '操作',
            valueType: 'option',
            render: (_, row) => [
              <Button
                key="edit"
                type="text"
                size="small"
                icon={<EditOutlined />}
                disabled={!canOperate}
                onClick={() => edit(row)}
              >
                编辑
              </Button>,
              <Button
                key="select"
                type="text"
                size="small"
                disabled={!canOperate || row.active}
                onClick={() => void prepare('select', { id: row.id })}
              >
                切换
              </Button>,
              <Button
                key="delete"
                type="text"
                size="small"
                danger
                disabled={!canOperate || row.active}
                onClick={() => void prepare('delete', { id: row.id })}
              >
                删除
              </Button>,
            ],
          },
        ]}
      />
      {editing && (
        <section className="panel connection-editor">
          <h2>{isNew ? '添加连接' : '编辑连接'}</h2>
          <Form
            form={form}
            layout="vertical"
            onValuesChange={() => setVerification(undefined)}
            onFinish={(values) => void prepare('save', values)}
          >
            <div className="schema-fields">
              <Form.Item name="id" label="连接 ID" rules={[{ required: true }]}>
                <Input disabled={!isNew} placeholder="tokyo-demo" />
              </Form.Item>
              <Form.Item name="name" label="名称" rules={[{ required: true }]}>
                <Input placeholder="名称" />
              </Form.Item>
              <Form.Item name="mode" label="环境" rules={[{ required: true }]}>
                <Select
                  options={[
                    { label: '模拟盘', value: 'demo' },
                    { label: '实盘 · 只读', value: 'live_readonly' },
                  ]}
                />
              </Form.Item>
              <Form.Item
                name="site"
                label="OKX 站点"
                rules={[{ required: true }]}
              >
                <Select
                  options={['global', 'eea', 'us', 'tr'].map((value) => ({
                    value,
                    label: value.toUpperCase(),
                  }))}
                />
              </Form.Item>
              <Form.Item
                name="apiKey"
                label="API Key"
                rules={[{ required: isNew }]}
              >
                <Input.Password
                  autoComplete="off"
                  placeholder={isNew ? '输入 API Key' : '留空保留原值'}
                />
              </Form.Item>
              <Form.Item
                name="secret"
                label="Secret Key"
                rules={[{ required: isNew }]}
              >
                <Input.Password
                  autoComplete="new-password"
                  placeholder={isNew ? '输入 Secret' : '留空保留原值'}
                />
              </Form.Item>
              <Form.Item
                name="passphrase"
                label="Passphrase"
                rules={[{ required: isNew }]}
              >
                <Input.Password
                  autoComplete="new-password"
                  placeholder={isNew ? '创建 API 时设置的口令' : '留空保留原值'}
                />
              </Form.Item>
            </div>
            <div className="native-actions">
              <Button
                loading={busy}
                icon={<LinkOutlined />}
                disabled={!canOperate}
                onClick={() => void test()}
              >
                测试连接
              </Button>
              <Button
                type="primary"
                htmlType="submit"
                disabled={!canOperate || !verification || busy}
              >
                保存
              </Button>
              <Button type="text" onClick={() => setEditing(false)}>
                取消
              </Button>
            </div>
            {verification && (
              <Alert
                type="success"
                className="inline-callout"
                title="连接正常"
                description={`账户 ${verification.uid} · ${verification.restMs} ms`}
              />
            )}
          </Form>
        </section>
      )}
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(value) => {
          setReceipt(value);
          if (value.status === 'completed') setEditing(false);
          void refresh();
        }}
      />
    </div>
  );
}
