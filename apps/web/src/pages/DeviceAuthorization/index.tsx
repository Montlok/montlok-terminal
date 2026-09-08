import { useEffect, useState } from 'react';
import { Button } from 'antd';
import { api } from '../../operator/api';
import './device.css';
type DeviceRequest={authorization_id:string;user_code:string;device_name:string;platform:string;public_key_fingerprint:string;approved:boolean};

export default function DeviceAuthorization(){
  const code=new URLSearchParams(window.location.search).get('code')||'';
  const [device,setDevice]=useState<DeviceRequest>();const [error,setError]=useState('');const [saving,setSaving]=useState(false);
  useEffect(()=>{
    if(!code){setError('请输入原生终端显示的授权链接');return;}
    let disposed=false;
    void api<DeviceRequest>(`v2/device/authorizations/${encodeURIComponent(code)}`).then((value)=>{if(!disposed)setDevice(value);}).catch((reason)=>{if(!disposed)setError(String(reason));});
    return()=>{disposed=true;};
  },[code]);
  const approve=async()=>{
    if(!device)return;setSaving(true);setError('');
    try{setDevice(await api<DeviceRequest>(`v2/device/authorizations/${encodeURIComponent(code)}/approve`,{}));}
    catch(reason){setError(reason instanceof Error?reason.message:String(reason));}finally{setSaving(false);}
  };
  return <main className="device-authorization">
    <section><p className="device-brand">MONTLOK TERMINAL</p><h1>{device?.approved?'设备已连接':'连接原生终端'}</h1>
      <p>{device?.approved?'返回原生终端继续使用。':'核对设备名称和授权码，为此设备授予当前账户的终端权限。'}</p>
      {error&&<p role="alert">{error}</p>}
      {device&&<><dl><dt>设备</dt><dd>{device.device_name}</dd><dt>平台</dt><dd>{device.platform}</dd><dt>授权码</dt><dd className="device-code">{device.user_code}</dd>
        <dt>设备密钥指纹</dt><dd className="device-fingerprint">{device.public_key_fingerprint}</dd></dl>
        {!device.approved&&<Button type="primary" loading={saving} onClick={()=>void approve()}>连接此设备</Button>}</>}
      {!device&&!error&&<p role="status">读取设备信息</p>}
    </section>
  </main>;
}
