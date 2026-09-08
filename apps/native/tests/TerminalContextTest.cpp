#include "TerminalContext.h"
#include "EventPresentation.h"
#include "EventBlotterModel.h"

#include <QSignalSpy>
#include <QtTest>

class TerminalContextTest final : public QObject
{
    Q_OBJECT
private Q_SLOTS:
    void combinedSelectionEmitsOneConsistentContext() {
        TerminalContext context;QSignalSpy changes(&context,&TerminalContext::contextChanged);
        context.setSelection(QStringLiteral("a"),QStringLiteral("g"),QStringLiteral("r"));
        QCOMPARE(changes.count(),1);QCOMPARE(context.accountId(),QStringLiteral("a"));QCOMPARE(context.runId(),QStringLiteral("r"));
        context.setSelection(QStringLiteral("a"),QStringLiteral("g"),QStringLiteral("r"));QCOMPARE(changes.count(),1);
    }
    void eventDetailsKeepExactNanoseconds() {
        QCOMPARE(exactEventTime(1800000000123456789LL).right(10),QStringLiteral(".123456789"));
        QCOMPARE(elapsedEventMs(1800000000123456789LL,1800000000123456790LL),QStringLiteral("0.000001"));
        QCOMPARE(elapsedEventMs(2,1),QStringLiteral("—"));
    }
    void highRateBlotterRemainsBoundedAndSelectable() {
        EventBlotterModel model;QList<QJsonObject> events;
        for(int n=0;n<25'000;++n)events.append(QJsonObject{{QStringLiteral("eventId"),QString::number(n)}});
        model.append(events);QCOMPARE(model.rowCount(),20'000);QCOMPARE(model.eventAt(0).value(QStringLiteral("eventId")).toString(),QStringLiteral("5000"));
        model.replace({});QCOMPARE(model.rowCount(),0);QVERIFY(model.eventAt(0).isEmpty());
    }
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
