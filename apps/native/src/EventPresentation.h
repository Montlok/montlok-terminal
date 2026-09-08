#pragma once
#include <QDateTime>
#include <QTimeZone>
#include <QString>
#include <QHash>
inline QString exactEventTime(qint64 ns) {
    if(ns<=0)return QStringLiteral("—");
    return QDateTime::fromSecsSinceEpoch(ns/1'000'000'000,QTimeZone("Asia/Shanghai")).toString(QStringLiteral("HH:mm:ss"))
        +QStringLiteral(".")+QString::number(ns%1'000'000'000).rightJustified(9,QChar(u'0'));
}
inline QString elapsedEventMs(qint64 start,qint64 end) {
    if(start<=0||end<start)return QStringLiteral("—");
    const auto delta=end-start;
    return QString::number(delta/1'000'000)+QStringLiteral(".")+QString::number(delta%1'000'000).rightJustified(6,QChar(u'0'));
}
inline QString eventTypeLabel(const QString &value) {
    static const QHash<QString,QString> labels{
        {QStringLiteral("ORDER_SUBMITTED"),QStringLiteral("委托提交")},{QStringLiteral("ORDER_ACCEPTED"),QStringLiteral("委托确认")},
        {QStringLiteral("ORDER_REJECTED"),QStringLiteral("委托拒绝")},{QStringLiteral("ORDER_CANCEL_REQUESTED"),QStringLiteral("撤单提交")},
        {QStringLiteral("ORDER_CANCELED"),QStringLiteral("撤单确认")},{QStringLiteral("FILL_RECEIVED"),QStringLiteral("成交")},
        {QStringLiteral("POSITION_UPDATED"),QStringLiteral("持仓更新")},{QStringLiteral("PNL_UPDATED"),QStringLiteral("收益更新")},
        {QStringLiteral("RUN_STATE_CHANGED"),QStringLiteral("运行状态")},{QStringLiteral("ROUTE_UPDATED"),QStringLiteral("路由更新")},
        {QStringLiteral("MODEL_INFERENCE_COMPLETED"),QStringLiteral("模型推理")},{QStringLiteral("SIGNAL_GENERATED"),QStringLiteral("信号")},
        {QStringLiteral("TARGET_POSITION_CHANGED"),QStringLiteral("目标更新")},{QStringLiteral("RISK_DECISION"),QStringLiteral("风控")},
        {QStringLiteral("ALERT_RAISED"),QStringLiteral("告警")},{QStringLiteral("ALERT_CLEARED"),QStringLiteral("告警恢复")},
        {QStringLiteral("OPERATION_RECEIPT_UPDATED"),QStringLiteral("操作回执")}};
    return labels.value(value,value);
}
