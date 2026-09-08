#pragma once

#include <QObject>
#include <QString>

class TerminalContext final : public QObject
{
    Q_OBJECT
    Q_PROPERTY(QString accountId READ accountId WRITE setAccountId NOTIFY contextChanged)
    Q_PROPERTY(QString strategyGroupId READ strategyGroupId WRITE setStrategyGroupId NOTIFY contextChanged)
    Q_PROPERTY(QString runId READ runId WRITE setRunId NOTIFY contextChanged)
    Q_PROPERTY(QString instrumentId READ instrumentId WRITE setInstrumentId NOTIFY contextChanged)
    Q_PROPERTY(QString modelReleaseId READ modelReleaseId WRITE setModelReleaseId NOTIFY contextChanged)
    Q_PROPERTY(QString signalVersion READ signalVersion WRITE setSignalVersion NOTIFY contextChanged)

public:
    explicit TerminalContext(QObject *parent = nullptr);

    QString accountId() const;
    QString strategyGroupId() const;
    QString runId() const;
    QString instrumentId() const;
    QString modelReleaseId() const;
    QString signalVersion() const;

    void setAccountId(const QString &value);
    void setStrategyGroupId(const QString &value);
    void setRunId(const QString &value);
    void setInstrumentId(const QString &value);
    void setModelReleaseId(const QString &value);
    void setSignalVersion(const QString &value);

Q_SIGNALS:
    void contextChanged();

private:
    void update(QString &field, const QString &value);
    QString m_accountId;
    QString m_strategyGroupId;
    QString m_runId;
    QString m_instrumentId;
    QString m_modelReleaseId;
    QString m_signalVersion;
};
