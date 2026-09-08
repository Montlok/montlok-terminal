#include "StrategyControlPanel.h"

#include "GatewayClient.h"
#include "TerminalContext.h"

#include <QFormLayout>
#include <QJsonObject>
#include <QLabel>
#include <QMessageBox>
#include <QPushButton>
#include <QVBoxLayout>

StrategyControlPanel::StrategyControlPanel(GatewayClient *gateway, TerminalContext *context, QWidget *parent)
    : QWidget(parent), m_gateway(gateway), m_context(context), m_state(new QLabel(QStringLiteral("等待运行上下文"), this))
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(10, 10, 10, 10);
    layout->setSpacing(8);
    m_state->setWordWrap(true);
    layout->addWidget(m_state);
    const QList<QPair<QString, QString>> actions = {
        {QStringLiteral("暂停开仓"), QStringLiteral("pause_opening")},
        {QStringLiteral("仅减仓"), QStringLiteral("reduce_only")},
        {QStringLiteral("恢复运行"), QStringLiteral("resume")},
        {QStringLiteral("撤单并暂停"), QStringLiteral("cancel_and_pause")},
        {QStringLiteral("停止运行"), QStringLiteral("stop")}
    };
    for (const auto &[label, action] : actions) {
        auto *button = new QPushButton(label, this);
        button->setProperty("operationAction", action);
        connect(button, &QPushButton::clicked, this, [this, action] { request(action); });
        layout->addWidget(button);
    }
    layout->addStretch();
    connect(context, &TerminalContext::contextChanged, this, [this] {
        m_state->setText(QStringLiteral("账户 %1\n策略组 %2\n运行 %3")
            .arg(m_context->accountId(), m_context->strategyGroupId(), m_context->runId()));
    });
    connect(gateway, &GatewayClient::operationPrepared, this, &StrategyControlPanel::confirm);
    connect(gateway, &GatewayClient::operationReceipt, this, [this](const QJsonObject &receipt) {
        m_state->setText(QStringLiteral("操作 %1\n状态 %2")
            .arg(receipt.value(QStringLiteral("operation_id")).toString(), receipt.value(QStringLiteral("status")).toString()));
    });
}

void StrategyControlPanel::request(const QString &action)
{
    if (m_context->accountId().isEmpty() || m_context->strategyGroupId().isEmpty() || m_context->runId().isEmpty()) {
        QMessageBox::information(this, QStringLiteral("运行上下文"), QStringLiteral("请先选择账户、策略组和运行实例。"));
        return;
    }
    m_state->setText(QStringLiteral("正在检查操作范围"));
    m_gateway->prepareOperation(action);
}

void StrategyControlPanel::confirm(const QJsonObject &prepared)
{
    const auto operationId = prepared.value(QStringLiteral("operation_id")).toString();
    const auto confirmationHash = prepared.value(QStringLiteral("confirmation_hash")).toString();
    const auto summary = prepared.value(QStringLiteral("summary")).toString();
    const auto context = prepared.value(QStringLiteral("context")).toObject();
    const auto matches=[this,&context]{return context.value(QStringLiteral("account_id")).toString()==m_context->accountId()
        &&context.value(QStringLiteral("strategy_group_id")).toString()==m_context->strategyGroupId()
        &&context.value(QStringLiteral("run_id")).toString()==m_context->runId();};
    if(!matches()){m_state->setText(QStringLiteral("运行选择已更新，请重新检查操作"));return;}
    const auto text = QStringLiteral("%1\n\n账户：%2\n策略组：%3\n运行：%4\n操作编号：%5")
        .arg(summary,
             context.value(QStringLiteral("account_id")).toString(),
             context.value(QStringLiteral("strategy_group_id")).toString(),
             context.value(QStringLiteral("run_id")).toString(), operationId);
    if (QMessageBox::question(this, QStringLiteral("确认操作"), text,
                              QMessageBox::Cancel | QMessageBox::Ok, QMessageBox::Cancel) == QMessageBox::Ok) {
        if(!matches()){m_state->setText(QStringLiteral("运行选择已更新，请重新检查操作"));return;}
        m_state->setText(QStringLiteral("操作已提交，正在查询回执"));
        m_gateway->executeOperation(operationId, confirmationHash);
    } else {
        m_state->setText(QStringLiteral("操作未提交"));
    }
}
