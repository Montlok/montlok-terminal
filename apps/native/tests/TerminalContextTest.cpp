#include "TerminalContext.h"

#include <QSignalSpy>
#include <QtTest>

class TerminalContextTest final : public QObject
{
    Q_OBJECT
private slots:
    void selectionNeverRepresentsAnExecutionCommand()
    {
        TerminalContext context;
        QSignalSpy changes(&context, &TerminalContext::contextChanged);
        context.setAccountId(QStringLiteral("account"));
        context.setStrategyGroupId(QStringLiteral("group"));
        context.setRunId(QStringLiteral("run"));
        context.setInstrumentId(QStringLiteral("BTC-USDT"));
        QCOMPARE(changes.count(), 4);
        QCOMPARE(context.runId(), QStringLiteral("run"));
        context.setRunId(QStringLiteral("run"));
        QCOMPARE(changes.count(), 4);
    }
};

QTEST_MAIN(TerminalContextTest)
#include "TerminalContextTest.moc"
