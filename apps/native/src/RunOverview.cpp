#include "RunOverview.h"
#include "GatewayClient.h"
#include "TerminalContext.h"
#include <QChart>
#include <QChartView>
#include <QComboBox>
#include <QDateTime>
#include <QDateTimeAxis>
#include <QFormLayout>
#include <QGridLayout>
#include <QHeaderView>
#include <QJsonObject>
#include <QLabel>
#include <QLineSeries>
#include <QMap>
#include <QSignalBlocker>
#include <QTableWidget>
#include <QTimer>
#include <QUrl>
#include <QValueAxis>
#include <QVBoxLayout>
#include <cmath>

namespace {
QString display(const QJsonValue &value){return value.isNull()||value.isUndefined()?QStringLiteral("—"):value.toVariant().toString();}
QString encoded(const QString &value){return QString::fromLatin1(QUrl::toPercentEncoding(value));}
QString stateLabel(const QString &state){static const QHash<QString,QString> labels{{QStringLiteral("running"),QStringLiteral("运行中")},{QStringLiteral("stopped"),QStringLiteral("已停止")},{QStringLiteral("starting"),QStringLiteral("启动中")},{QStringLiteral("ready"),QStringLiteral("待运行")},{QStringLiteral("completed"),QStringLiteral("已结束")},{QStringLiteral("reducing"),QStringLiteral("仅减仓")},{QStringLiteral("halted"),QStringLiteral("已暂停")}};return labels.value(state,state);}
}
RunOverview::RunOverview(GatewayClient *gateway,TerminalContext *context,QObject *parent):QObject(parent),m_gateway(gateway),m_context(context),m_metricsWidget(new QWidget),m_chartWidget(new QWidget),m_groups(new QComboBox),m_runs(new QComboBox),m_source(new QLabel),m_universe(new QTableWidget),m_chart(new QChart),m_timer(new QTimer(this)){
    auto *layout=new QVBoxLayout(m_metricsWidget);layout->setContentsMargins(8,8,8,8);
    auto *form=new QFormLayout;m_groups->setAccessibleName(QStringLiteral("策略组"));m_runs->setAccessibleName(QStringLiteral("运行实例"));form->addRow(QStringLiteral("策略组"),m_groups);form->addRow(QStringLiteral("运行实例"),m_runs);layout->addLayout(form);
    const QList<QPair<QString,QString>> metrics{{QStringLiteral("nav"),QStringLiteral("NAV / USDT")},{QStringLiteral("pnl"),QStringLiteral("PnL / USDT")},{QStringLiteral("returnPct"),QStringLiteral("收益率 / %")},{QStringLiteral("maxDrawdownPct"),QStringLiteral("最大回撤 / %")},{QStringLiteral("fees"),QStringLiteral("费用 / USDT")},{QStringLiteral("fillsTotal"),QStringLiteral("成交笔数")}};
    auto *grid=new QGridLayout;int index=0;
    for(const auto &[key,title]:metrics){auto *cell=new QWidget;auto *column=new QVBoxLayout(cell);column->setContentsMargins(4,4,4,4);column->addWidget(new QLabel(title));auto *value=new QLabel(QStringLiteral("—"));value->setProperty("numeric",true);value->setObjectName(key);value->setStyleSheet(QStringLiteral("font-size:17px;font-weight:600;"));column->addWidget(value);grid->addWidget(cell,index/3,index%3);m_metrics.insert(key,value);++index;}
    layout->addLayout(grid);m_source->setWordWrap(true);m_source->setText(QStringLiteral("等待运行快照"));layout->addWidget(m_source);
    m_universe->setColumnCount(6);m_universe->setHorizontalHeaderLabels({QStringLiteral("品种"),QStringLiteral("板块"),QStringLiteral("目标权重"),QStringLiteral("实际持仓"),QStringLiteral("名义敞口 / USDT"),QStringLiteral("未实现 PnL / USDT")});m_universe->setEditTriggers(QAbstractItemView::NoEditTriggers);m_universe->setSelectionBehavior(QAbstractItemView::SelectRows);m_universe->setAlternatingRowColors(true);m_universe->verticalHeader()->setDefaultSectionSize(28);m_universe->horizontalHeader()->setStretchLastSection(true);
    auto *chartLayout=new QVBoxLayout(m_chartWidget);chartLayout->setContentsMargins(4,4,4,4);m_chart->setTitle(QStringLiteral("组合净值 / USDT"));m_chart->legend()->hide();m_chart->setTheme(QChart::ChartThemeDark);m_chart->setBackgroundBrush(QColor(QStringLiteral("#10161d")));auto *view=new QChartView(m_chart);chartLayout->addWidget(view);
    connect(gateway,&GatewayClient::bootstrapReceived,this,[this]{m_gateway->getLegacy(QStringLiteral("strategy-groups"),QStringLiteral("overview/groups"));refresh();m_timer->start(3000);});
    connect(gateway,&GatewayClient::datasetReceived,this,&RunOverview::receive);
    connect(gateway,&GatewayClient::requestFailed,this,[this](const QString &tag,const QString &error){if(!m_request.isEmpty()&&tag.startsWith(QStringLiteral("overview/"))&&tag.endsWith(QStringLiteral("/")+m_request)){m_pending=std::max(0,m_pending-1);m_busy=m_pending>0;m_source->setText(QStringLiteral("运行快照读取失败：")+error);}});
    connect(m_groups,&QComboBox::activated,this,[this]{selectGroup();});
    connect(m_runs,&QComboBox::activated,this,[this]{m_context->setSelection(m_context->accountId(),m_context->strategyGroupId(),m_runs->currentData().toString());});
    connect(context,&TerminalContext::contextChanged,this,[this]{
        const auto scope=m_context->strategyGroupId()+QStringLiteral("/")+m_context->runId();if(scope==m_scope)return;m_scope=scope;m_busy=false;
        for(auto *value:m_metrics)value->setText(QStringLiteral("—"));m_universe->setRowCount(0);m_chart->removeAllSeries();m_lastEquity={};m_source->setText(QStringLiteral("正在读取运行快照"));refresh();
    });
    connect(m_timer,&QTimer::timeout,this,&RunOverview::refresh);
    connect(m_universe,&QTableWidget::currentCellChanged,this,[this](int row){if(row>=0&&m_universe->item(row,0))m_context->setInstrumentId(m_universe->item(row,0)->text());});
}
QWidget *RunOverview::universeWidget()const{return m_universe;}
void RunOverview::selectGroup(){
    const auto id=m_groups->currentData().toString();for(const auto &value:m_catalog){const auto group=value.toObject();if(group.value(QStringLiteral("id")).toString()==id){m_context->setSelection(group.value(QStringLiteral("accountId")).toString(),id,group.value(QStringLiteral("runId")).toString());break;}}
}
void RunOverview::refresh(){
    if(m_busy||m_context->strategyGroupId().isEmpty())return;m_busy=true;
    const auto path=QStringLiteral("strategy-groups/")+encoded(m_context->strategyGroupId());const auto run=QStringLiteral("runId=")+encoded(m_context->runId());
    m_scope=m_context->strategyGroupId()+QStringLiteral("/")+m_context->runId();
    m_request=m_scope+QStringLiteral("#")+QString::number(++m_generation);m_pending=3;
    m_gateway->getLegacy(path+QStringLiteral("?")+run+QStringLiteral("&view=summary"),QStringLiteral("overview/summary/")+m_request);
    m_gateway->getLegacy(path+QStringLiteral("/equity?")+run,QStringLiteral("overview/equity/")+m_request);
    m_gateway->getLegacy(path+QStringLiteral("/runtime"),QStringLiteral("overview/runtime/")+m_request);
}
void RunOverview::receive(const QString &tag,const QJsonValue &data){
    const auto object=data.toObject();
    if(tag==QStringLiteral("overview/groups")){
        m_catalog=object.value(QStringLiteral("groups")).toArray();QSignalBlocker blocker(m_groups);m_groups->clear();
        for(const auto &value:m_catalog){const auto group=value.toObject();m_groups->addItem(group.value(QStringLiteral("name")).toString(),group.value(QStringLiteral("id")).toString());}
        const int index=m_groups->findData(m_context->strategyGroupId());if(index>=0)m_groups->setCurrentIndex(index);else if(m_groups->count())selectGroup();return;
    }
    if(m_request.isEmpty()||!tag.endsWith(QStringLiteral("/")+m_request))return;
    m_pending=std::max(0,m_pending-1);m_busy=m_pending>0;
    if(tag.startsWith(QStringLiteral("overview/summary/"))){
        if(object.value(QStringLiteral("id")).toString()!=m_context->strategyGroupId()||object.value(QStringLiteral("runId")).toString()!=m_context->runId()){m_source->setText(QStringLiteral("运行实例数据不一致，正在等待所选实例"));return;}
        const auto metrics=object.value(QStringLiteral("metrics")).toObject();const auto sources=object.value(QStringLiteral("sources")).toObject();
        for(auto it=m_metrics.begin();it!=m_metrics.end();++it){const auto value=it.key()==QStringLiteral("fillsTotal")?object.value(it.key()):metrics.value(it.key());
            const bool unavailable=it.key()==QStringLiteral("fillsTotal")?sources.value(QStringLiteral("fills"))==false:sources.value(QStringLiteral("accounting"))==false;
            it.value()->setText(unavailable?QStringLiteral("—"):display(value));
        }
        m_source->setText(stateLabel(object.value(QStringLiteral("status")).toString())+QStringLiteral(" · ")+display(object.value(QStringLiteral("accountId")))+QStringLiteral(" · ")+display(object.value(QStringLiteral("marketSource")))+QStringLiteral("\n运行 ")+m_context->runId());
        QMap<QString,QJsonObject> universe;for(const auto &value:object.value(QStringLiteral("universe")).toArray()){const auto row=value.toObject();universe.insert(row.value(QStringLiteral("instrument")).toString(),row);}
        const bool positions=sources.value(QStringLiteral("positions"))!=false&&sources.value(QStringLiteral("view"))!=false;
        if(positions)for(const auto &value:object.value(QStringLiteral("positions")).toArray()){const auto row=value.toObject();const auto symbol=row.value(QStringLiteral("instrument")).toString();auto merged=universe.value(symbol);for(auto it=row.begin();it!=row.end();++it)merged.insert(it.key(),it.value());universe.insert(symbol,merged);}
        const auto selected=m_context->instrumentId();QSignalBlocker blocker(m_universe);m_universe->setRowCount(universe.size());int rowIndex=0;
        for(auto it=universe.begin();it!=universe.end();++it){const auto &row=it.value();const QStringList values{it.key(),display(row.value(QStringLiteral("sector"))),display(row.value(QStringLiteral("weight"))),positions?display(row.value(QStringLiteral("quantity"))):QStringLiteral("—"),positions?display(row.value(QStringLiteral("notional"))):QStringLiteral("—"),positions?display(row.value(QStringLiteral("unrealizedPnl"))):QStringLiteral("—")};
            for(int column=0;column<values.size();++column)m_universe->setItem(rowIndex,column,new QTableWidgetItem(values[column]));if(it.key()==selected)m_universe->selectRow(rowIndex);++rowIndex;
        }
    }else if(tag.startsWith(QStringLiteral("overview/runtime/"))){
        QSignalBlocker blocker(m_runs);m_runs->clear();for(const auto &value:object.value(QStringLiteral("groups")).toArray()){
            const auto group=value.toObject();if(group.value(QStringLiteral("groupId")).toString()!=m_context->strategyGroupId())continue;
            for(const auto &item:group.value(QStringLiteral("runs")).toArray()){const auto run=item.toObject();const auto id=run.value(QStringLiteral("runId")).toString();m_runs->addItem(id+QStringLiteral(" · ")+stateLabel(run.value(QStringLiteral("status")).toString()),id);}
        }
        int index=m_runs->findData(m_context->runId());if(index<0&&!m_context->runId().isEmpty()){m_runs->addItem(m_context->runId(),m_context->runId());index=m_runs->count()-1;}m_runs->setCurrentIndex(index);
    }else if(tag.startsWith(QStringLiteral("overview/equity/"))){
        if(object.value(QStringLiteral("runId")).toString()!=m_context->runId())return;
        const auto points=object.value(QStringLiteral("points")).toArray();if(points==m_lastEquity)return;m_lastEquity=points;
        m_chart->removeAllSeries();const auto axes=m_chart->axes();for(auto *axis:axes){m_chart->removeAxis(axis);delete axis;}
        auto *series=new QLineSeries;
        for(const auto &value:points){const auto point=value.toObject();bool valid=false;const double amount=point.value(QStringLiteral("value")).toVariant().toDouble(&valid);const auto timestamp=point.value(QStringLiteral("time")).toVariant().toLongLong();if(valid&&std::isfinite(amount)&&timestamp>0)series->append(timestamp*1000.,amount);}
        if(!series->count()){delete series;m_chart->setTitle(QStringLiteral("组合净值 / USDT · 等待观测数据"));return;}
        series->setColor(QColor(QStringLiteral("#80aaff")));m_chart->addSeries(series);auto *time=new QDateTimeAxis;time->setFormat(QStringLiteral("HH:mm"));auto *amount=new QValueAxis;m_chart->addAxis(time,Qt::AlignBottom);m_chart->addAxis(amount,Qt::AlignRight);series->attachAxis(time);series->attachAxis(amount);
        const auto first=series->at(0),last=series->at(series->count()-1);time->setRange(QDateTime::fromMSecsSinceEpoch(first.x()),QDateTime::fromMSecsSinceEpoch(std::max(last.x(),first.x()+1000)));
        double low=first.y(),high=low;for(const auto &point:series->points()){low=std::min(low,point.y());high=std::max(high,point.y());}const auto pad=std::max(0.01,(high-low)*0.08);amount->setRange(low-pad,high+pad);m_chart->setTitle(QStringLiteral("组合净值 / USDT · %1 条观测").arg(points.size()));
    }
}
