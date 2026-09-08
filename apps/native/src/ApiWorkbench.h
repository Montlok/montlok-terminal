#pragma once
#include <QWidget>
#include <QJsonObject>
class GatewayClient;class QComboBox;class QPlainTextEdit;class QPushButton;
class ApiWorkbench final:public QWidget{
public:
    explicit ApiWorkbench(GatewayClient *gateway,QWidget *parent=nullptr);
private:
    void refresh();void prepare();void selected();void receive(const QString &tag,const QJsonValue &value);
    GatewayClient *m_gateway;QComboBox *m_operations;QPlainTextEdit *m_input;QPlainTextEdit *m_output;
    QPushButton *m_submit;QPushButton *m_receipt;QJsonObject m_ticket;
};
