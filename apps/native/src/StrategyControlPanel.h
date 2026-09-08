#pragma once

#include <QWidget>

class GatewayClient;
class QLabel;
class TerminalContext;

class StrategyControlPanel final : public QWidget
{
    Q_OBJECT
public:
    StrategyControlPanel(GatewayClient *gateway, TerminalContext *context, QWidget *parent = nullptr);

private:
    void request(const QString &action);
    void confirm(const QJsonObject &prepared);
    GatewayClient *m_gateway;
    TerminalContext *m_context;
    QLabel *m_state;
};
