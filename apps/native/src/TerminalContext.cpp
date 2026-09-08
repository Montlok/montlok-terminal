#include "TerminalContext.h"

TerminalContext::TerminalContext(QObject *parent) : QObject(parent) {}

QString TerminalContext::accountId() const { return m_accountId; }
QString TerminalContext::strategyGroupId() const { return m_strategyGroupId; }
QString TerminalContext::runId() const { return m_runId; }
QString TerminalContext::instrumentId() const { return m_instrumentId; }
QString TerminalContext::modelReleaseId() const { return m_modelReleaseId; }
QString TerminalContext::signalVersion() const { return m_signalVersion; }

void TerminalContext::update(QString &field, const QString &value)
{
    if (field == value)
        return;
    field = value;
    Q_EMIT contextChanged();
}

void TerminalContext::setAccountId(const QString &value) { update(m_accountId, value); }
void TerminalContext::setStrategyGroupId(const QString &value) { update(m_strategyGroupId, value); }
void TerminalContext::setRunId(const QString &value) { update(m_runId, value); }
void TerminalContext::setInstrumentId(const QString &value) { update(m_instrumentId, value); }
void TerminalContext::setModelReleaseId(const QString &value) { update(m_modelReleaseId, value); }
void TerminalContext::setSignalVersion(const QString &value) { update(m_signalVersion, value); }
void TerminalContext::setSelection(const QString &account,const QString &group,const QString &run) {
    if(m_accountId==account&&m_strategyGroupId==group&&m_runId==run)return;
    m_accountId=account;m_strategyGroupId=group;m_runId=run;m_instrumentId.clear();m_modelReleaseId.clear();m_signalVersion.clear();
    Q_EMIT contextChanged();
}
