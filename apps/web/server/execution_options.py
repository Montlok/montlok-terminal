"""Versioned, non-executable settings for the native-backed Alpha executor."""
MODES = {
    'continuous_quotes': ('持续报价', '保留 Alpha 目标权重，在库存带内持续挂单并随行情更新。'),
    'continuous_rebalance': ('持续再平衡', '随行情更新目标数量，用挂单调整偏离的持仓。'),
    'initial_allocation': ('单次调仓', '按启动时的目标数量完成一次调仓。'),
}
DEFAULTS = dict(mode='continuous_quotes', quoteIntervalMs=100, requoteThresholdBps=2,
                quoteSizeLots=1, inventoryBandLots=1, orderExpirySecs=30,
                maxOrdersPerSecond=100, minimumEdgeBps=4, maxOrderNotionalUsdt=10)
# Timing and integer bounds describe the implemented protocol. The venue's
# instrument and account limiters remain authoritative for actual requests.
BOUNDS = dict(quoteIntervalMs=(10,60000), requoteThresholdBps=(0,10000),
              quoteSizeLots=(1,1000000), inventoryBandLots=(1,1000000),
              orderExpirySecs=(1,86400), maxOrdersPerSecond=(1,500), minimumEdgeBps=(0,10000),
              maxOrderNotionalUsdt=(1,1000))


def settings(spec, supplied=None):
    if spec.get('executionOptionsVersion') != 1:
        if supplied is not None:
            raise ValueError('此运行程序尚未发布执行参数接口')
        return None
    if supplied is not None and (not isinstance(supplied,dict) or set(supplied)-set(DEFAULTS)):
        raise ValueError('执行参数包含未定义字段')
    value = {**DEFAULTS, **(supplied or {})}
    if value['mode'] not in MODES:
        raise ValueError('请选择已发布的执行方式')
    for key,(low,high) in BOUNDS.items():
        item=value[key]
        if type(item) is not int or not low<=item<=high:
            raise ValueError(f'{key} 必须为 {low} 至 {high} 的整数')
    if value['inventoryBandLots'] < value['quoteSizeLots']:
        raise ValueError('库存带需容纳至少一笔报价数量')
    return value


def describe(value):
    if not value:return None
    label,description=MODES[value['mode']]
    return dict(kind=value['mode'],label=label,description=description)


def contract(spec):
    value=settings(spec)
    return (dict(version=1,defaults=value,bounds=BOUNDS,
                 modes=[dict(value=key,label=row[0]) for key,row in MODES.items()]) if value else None)


def rate_config(config, value):
    if not value:return config
    return {**config,'maxOrderSubmitRate':f"{value['maxOrdersPerSecond']}/00:00:01"}
