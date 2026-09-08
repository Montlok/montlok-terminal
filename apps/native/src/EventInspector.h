#pragma once
#include <QWidget>
#include <QJsonObject>
class GatewayClient;
class QTableWidget;
class QPlainTextEdit;
class QLabel;
class EventInspector final: public QWidget {
public:
    explicit EventInspector(GatewayClient *gateway,QWidget *parent=nullptr);
    void selectEvent(const QJsonObject &event);
private:
    GatewayClient *m_gateway;
    QTableWidget *m_fields;
    QTableWidget *m_timeline;
    QPlainTextEdit *m_raw;
    QLabel *m_status;
    QJsonObject m_selected;
};
