#pragma once
#include <QObject>
#include <QJsonArray>
#include <QHash>
class GatewayClient;
class TerminalContext;
class QWidget;
class QComboBox;
class QLabel;
class QTableWidget;
class QChart;
class QTimer;
class RunOverview final:public QObject {
public:
    RunOverview(GatewayClient *gateway,TerminalContext *context,QObject *parent=nullptr);
    QWidget *metricsWidget()const{return m_metricsWidget;}
    QWidget *universeWidget()const;
    QWidget *chartWidget()const{return m_chartWidget;}
private:
    void refresh();
    void receive(const QString &tag,const QJsonValue &data);
    void selectGroup();
    GatewayClient *m_gateway;
    TerminalContext *m_context;
    QWidget *m_metricsWidget;
    QWidget *m_chartWidget;
    QComboBox *m_groups;
    QComboBox *m_runs;
    QLabel *m_source;
    QTableWidget *m_universe;
    QChart *m_chart;
    QHash<QString,QLabel*> m_metrics;
    QJsonArray m_catalog;
    QTimer *m_timer;
    QString m_scope;
    QString m_request;
    int m_generation=0;
    int m_pending=0;
    QJsonArray m_lastEquity;
    bool m_busy=false;
};
