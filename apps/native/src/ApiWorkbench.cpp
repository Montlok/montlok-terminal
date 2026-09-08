#include "ApiWorkbench.h"
#include "GatewayClient.h"
#include <QComboBox>
#include <QDateTime>
#include <QJsonArray>
#include <QJsonDocument>
#include <QLabel>
#include <QMessageBox>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>

ApiWorkbench::ApiWorkbench(GatewayClient *gateway,QWidget *parent):QWidget(parent),m_gateway(gateway),
    m_operations(new QComboBox(this)),m_input(new QPlainTextEdit(this)),m_output(new QPlainTextEdit(this)),
    m_submit(new QPushButton(QStringLiteral("检查参数"),this)),m_receipt(new QPushButton(QStringLiteral("查询回执"),this)){
    auto *layout=new QVBoxLayout(this);layout->setContentsMargins(10,10,10,10);
    auto *reload=new QPushButton(QStringLiteral("读取接口目录"),this);connect(reload,&QPushButton::clicked,this,[this]{refresh();});
    layout->addWidget(reload);layout->addWidget(m_operations);layout->addWidget(new QLabel(QStringLiteral("参数 JSON"),this));
    m_input->setPlainText(QStringLiteral("{}"));layout->addWidget(m_input);layout->addWidget(m_submit);layout->addWidget(m_receipt);
    m_submit->setEnabled(false);m_receipt->setEnabled(false);
    m_output->setReadOnly(true);layout->addWidget(m_output);
    connect(m_operations,&QComboBox::currentIndexChanged,this,[this]{selected();});
    connect(m_submit,&QPushButton::clicked,this,[this]{prepare();});
    connect(m_receipt,&QPushButton::clicked,this,[this]{const auto id=m_ticket.value(QStringLiteral("id")).toString();if(!id.isEmpty())m_gateway->getLegacy(QStringLiteral("operations/")+id,QStringLiteral("workbench.receipt"));});
    connect(gateway,&GatewayClient::datasetReceived,this,[this](const QString &tag,const QJsonValue &value){receive(tag,value);});
    connect(gateway,&GatewayClient::requestFailed,this,[this](const QString &tag,const QString &message){if(tag.startsWith(QStringLiteral("workbench."))){m_output->setPlainText(message);m_submit->setEnabled(true);}});
}
void ApiWorkbench::refresh(){m_gateway->getLegacy(QStringLiteral("catalog"),QStringLiteral("workbench.catalog"));}
void ApiWorkbench::selected(){
    const auto spec=m_operations->currentData().toJsonObject();const auto schema=spec.value(QStringLiteral("inputSchema")).toObject();
    m_output->setPlainText(QString::fromUtf8(QJsonDocument(schema).toJson(QJsonDocument::Indented)));m_ticket={};
    m_submit->setEnabled(!spec.isEmpty());m_receipt->setEnabled(false);
}
void ApiWorkbench::prepare(){
    QJsonParseError error;const auto document=QJsonDocument::fromJson(m_input->toPlainText().toUtf8(),&error);
    if(error.error!=QJsonParseError::NoError||!document.isObject()){m_output->setPlainText(QStringLiteral("请输入有效的参数对象"));return;}
    const auto spec=m_operations->currentData().toJsonObject();const auto name=spec.value(QStringLiteral("name")).toString();if(name.isEmpty())return;
    const auto kind=spec.value(QStringLiteral("kind")).toString();
    const bool readOnly=kind==QStringLiteral("rest")?name.startsWith(QStringLiteral("GET ")):spec.value(QStringLiteral("annotations")).toObject().value(QStringLiteral("readOnlyHint")).toBool();
    m_submit->setEnabled(false);m_gateway->postLegacy(readOnly?QStringLiteral("query"):QStringLiteral("prepare"),
        {{QStringLiteral("kind"),kind},{QStringLiteral("name"),name},{QStringLiteral("arguments"),document.object()}},
        readOnly?QStringLiteral("workbench.result"):QStringLiteral("workbench.prepare"));
}
void ApiWorkbench::receive(const QString &tag,const QJsonValue &value){
    if(!tag.startsWith(QStringLiteral("workbench.")))return;
    if(tag==QStringLiteral("workbench.catalog")){
        m_operations->clear();const auto catalog=value.toObject();
        for(const auto &key:{QStringLiteral("tools"),QStringLiteral("routes")})for(const auto &entry:catalog.value(key).toArray()){
            auto object=entry.toObject();object.insert(QStringLiteral("kind"),key==QStringLiteral("tools")?QStringLiteral("mcp"):QStringLiteral("rest"));
            auto name=object.value(QStringLiteral("name")).toString();if(name.isEmpty())name=object.value(QStringLiteral("method")).toString()+QStringLiteral(" ")+object.value(QStringLiteral("path")).toString();
            object.insert(QStringLiteral("name"),name);m_operations->addItem(name,object);
        }return;
    }
    m_submit->setEnabled(true);
    const auto document=value.isArray()?QJsonDocument(value.toArray()):QJsonDocument(value.toObject());
    m_output->setPlainText(QString::fromUtf8(document.toJson(QJsonDocument::Indented)));
    if(tag==QStringLiteral("workbench.prepare")){
        m_ticket=value.toObject();const auto operation=m_ticket.value(QStringLiteral("operation")).toObject();
        const auto text=QString::fromUtf8(QJsonDocument(QJsonObject{{QStringLiteral("账户"),m_ticket.value(QStringLiteral("profile"))},
            {QStringLiteral("操作"),operation},{QStringLiteral("执行范围"),m_ticket.value(QStringLiteral("groupPreview"))}}).toJson(QJsonDocument::Indented));
        if(QMessageBox::question(this,QStringLiteral("确认操作"),text,QMessageBox::Cancel|QMessageBox::Ok,QMessageBox::Cancel)==QMessageBox::Ok){
            if(m_ticket.value(QStringLiteral("expiresAt")).toDouble()<=QDateTime::currentSecsSinceEpoch()){m_output->setPlainText(QStringLiteral("确认已过期，请重新检查参数"));return;}
            m_submit->setEnabled(false);m_receipt->setEnabled(true);m_gateway->postLegacy(QStringLiteral("execute"),{{QStringLiteral("id"),m_ticket.value(QStringLiteral("id"))}},QStringLiteral("workbench.receipt"));
        }
    }
}
