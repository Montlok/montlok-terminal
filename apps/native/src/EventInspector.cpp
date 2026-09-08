#include "EventInspector.h"
#include "EventPresentation.h"
#include "GatewayClient.h"
#include <QHeaderView>
#include <QJsonDocument>
#include <QLabel>
#include <QPlainTextEdit>
#include <QTableWidget>
#include <QTabWidget>
#include <QVBoxLayout>

namespace {
QString text(const QJsonValue &value){return value.isUndefined()||value.isNull()?QStringLiteral("—"):value.toVariant().toString();}
void configure(QTableWidget *table,const QStringList &headers){
    table->setColumnCount(headers.size());table->setHorizontalHeaderLabels(headers);table->verticalHeader()->hide();
    table->setEditTriggers(QAbstractItemView::NoEditTriggers);table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->horizontalHeader()->setStretchLastSection(true);table->verticalHeader()->setDefaultSectionSize(28);
}
}
EventInspector::EventInspector(GatewayClient *gateway,QWidget *parent):QWidget(parent),m_gateway(gateway),m_fields(new QTableWidget(this)),m_timeline(new QTableWidget(this)),m_raw(new QPlainTextEdit(this)),m_status(new QLabel(QStringLiteral("选择事件、委托或成交"),this)){
    auto *layout=new QVBoxLayout(this);layout->setContentsMargins(6,6,6,6);auto *tabs=new QTabWidget(this);
    configure(m_fields,{QStringLiteral("字段"),QStringLiteral("值")});configure(m_timeline,{QStringLiteral("时间 / UTC+8"),QStringLiteral("阶段"),QStringLiteral("间隔 / ms"),QStringLiteral("来源"),QStringLiteral("序号")});
    m_raw->setReadOnly(true);m_raw->setLineWrapMode(QPlainTextEdit::WidgetWidth);
    tabs->addTab(m_fields,QStringLiteral("摘要与来源"));tabs->addTab(m_timeline,QStringLiteral("关联时间线"));tabs->addTab(m_raw,QStringLiteral("原始字段"));layout->addWidget(tabs);layout->addWidget(m_status);
    connect(gateway,&GatewayClient::relatedReceived,this,[this](const QString &id,const QList<QJsonObject> &events,bool truncated){
        if(id!=m_selected.value(QStringLiteral("eventId")).toString())return;
        m_timeline->setRowCount(0);qint64 previous=0;
        for(const auto &event:events){
            if(event.value(QStringLiteral("accountId"))!=m_selected.value(QStringLiteral("accountId"))||event.value(QStringLiteral("runId"))!=m_selected.value(QStringLiteral("runId")))continue;
            const auto ns=event.value(QStringLiteral("occurredAtNs")).toVariant().toLongLong();
            const QStringList values{exactEventTime(ns),eventTypeLabel(event.value(QStringLiteral("eventType")).toString()),previous?elapsedEventMs(previous,ns):QStringLiteral("起点"),text(event.value(QStringLiteral("source"))),text(event.value(QStringLiteral("streamSeq")))};
            const auto row=m_timeline->rowCount();m_timeline->insertRow(row);for(int col=0;col<values.size();++col)m_timeline->setItem(row,col,new QTableWidgetItem(values[col]));previous=ns;
        }
        m_timeline->resizeColumnsToContents();m_status->setText(truncated?QStringLiteral("关联记录超过 500 条，当前显示部分历史。"):QStringLiteral("已读取 %1 条关联事件").arg(m_timeline->rowCount()));
    });
    connect(gateway,&GatewayClient::requestFailed,this,[this](const QString &tag,const QString &error){if(tag==QStringLiteral("detail/")+m_selected.value(QStringLiteral("eventId")).toString())m_status->setText(QStringLiteral("关联历史暂时无法读取：")+error);});
}
void EventInspector::selectEvent(const QJsonObject &event){
    m_selected=event;m_fields->setRowCount(0);m_timeline->setRowCount(0);
    auto add=[this](const QString &label,const QString &value){const int row=m_fields->rowCount();m_fields->insertRow(row);m_fields->setItem(row,0,new QTableWidgetItem(label));m_fields->setItem(row,1,new QTableWidgetItem(value.isEmpty()?QStringLiteral("—"):value));};
    add(QStringLiteral("事件"),eventTypeLabel(event.value(QStringLiteral("eventType")).toString()));
    const auto occurred=event.value(QStringLiteral("occurredAtNs")).toVariant().toLongLong();const auto received=event.value(QStringLiteral("receivedAtNs")).toVariant().toLongLong();
    add(QStringLiteral("发生时间 / UTC+8"),exactEventTime(occurred));add(QStringLiteral("接收时间 / UTC+8"),exactEventTime(received));add(QStringLiteral("采集耗时 / ms"),elapsedEventMs(occurred,received));
    const QList<QPair<QString,QString>> keys{{QStringLiteral("账户"),QStringLiteral("accountId")},{QStringLiteral("策略组"),QStringLiteral("strategyGroupId")},{QStringLiteral("运行实例"),QStringLiteral("runId")},{QStringLiteral("品种"),QStringLiteral("instrumentId")},{QStringLiteral("Correlation ID"),QStringLiteral("correlationId")},{QStringLiteral("Causation ID"),QStringLiteral("causationId")},{QStringLiteral("事件编号"),QStringLiteral("eventId")},{QStringLiteral("来源"),QStringLiteral("source")},{QStringLiteral("数据流"),QStringLiteral("stream")},{QStringLiteral("流序号"),QStringLiteral("streamSeq")}};
    for(const auto &[label,key]:keys)add(label,text(event.value(key)));
    for(const auto &name:{QStringLiteral("orderEvent"),QStringLiteral("fillReceived"),QStringLiteral("routeUpdated")}){
        const auto value=event.value(name).toObject();
        for(auto it=value.begin();it!=value.end();++it)add(it.key(),it.value().isObject()?text(it.value().toObject().value(QStringLiteral("value"))):text(it.value()));
    }
    m_raw->setPlainText(QString::fromUtf8(QJsonDocument(event).toJson(QJsonDocument::Indented)));m_fields->resizeColumnToContents(0);m_status->setText(QStringLiteral("正在查询关联历史"));
    m_gateway->queryRelated(event.value(QStringLiteral("eventId")).toString());
}
