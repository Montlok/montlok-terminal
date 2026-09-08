#include "MainWindow.h"

#include "ArrowTableModel.h"
#include "ApiWorkbench.h"
#include "EventBlotterModel.h"
#include "CommandPalette.h"
#include "GatewayClient.h"
#include "StrategyControlPanel.h"
#include "TerminalContext.h"

#include <kddockwidgets/DockWidget.h>
#include <kddockwidgets/LayoutSaver.h>

#include <QApplication>
#include <QChart>
#include <QChartView>
#include <QCloseEvent>
#include <QComboBox>
#include <QDir>
#include <QFormLayout>
#include <QHeaderView>
#include <QJsonArray>
#include <QLabel>
#include <QMenuBar>
#include <QMessageBox>
#include <QShortcut>
#include <QStandardItemModel>
#include <QStandardPaths>
#include <QStatusBar>
#include <QTableView>
#include <QToolBar>
#include <QVBoxLayout>

namespace {
QLabel *valueLabel(const QString &value = QStringLiteral("—"))
{
    auto *label = new QLabel(value);
    label->setProperty("numeric", true);
    label->setTextInteractionFlags(Qt::TextSelectableByMouse);
    return label;
}

QString displayTime(qint64 nanoseconds)
{
    return QDateTime::fromMSecsSinceEpoch(nanoseconds / 1'000'000, Qt::UTC)
        .toLocalTime().toString(QStringLiteral("yyyy-MM-dd HH:mm:ss.zzz"));
}
}

MainWindow::MainWindow(const QUrl &gatewayUrl, QWidget *parent)
    : KDDockWidgets::QtWidgets::MainWindow(QStringLiteral("MontlokTerminalMain"),
          KDDockWidgets::MainWindowOption_HasCentralWidget, parent),
      m_context(new TerminalContext(this)),
      m_gateway(new GatewayClient(gatewayUrl, m_context, this)),
      m_palette(new CommandPalette(this)),
      m_connection(new QLabel(QStringLiteral("尚未连接"), this)),
      m_contextLabel(new QLabel(QStringLiteral("账户 — · 策略组 — · 运行 —"), this)),
      m_blotterModel(new EventBlotterModel(this))
{
    setWindowTitle(QStringLiteral("Montlok Terminal"));
    resize(1720, 1040);
    createMenus();
    createPanels();
    statusBar()->addWidget(m_connection);
    statusBar()->addPermanentWidget(m_contextLabel, 1);

    connect(m_gateway, &GatewayClient::connectionStateChanged, m_connection, &QLabel::setText);
    connect(m_gateway, &GatewayClient::bootstrapReceived, this, &MainWindow::applyBootstrap);
    connect(m_gateway, &GatewayClient::eventReceived, this, &MainWindow::appendEvent);
    connect(m_gateway,&GatewayClient::initialSnapshotReceived,this,[this](const QList<QJsonObject> &events){
        m_pendingEvents.clear();m_blotterModel->replace(events);
    });
    auto *frameTimer=new QTimer(this);frameTimer->setInterval(50);
    connect(frameTimer,&QTimer::timeout,this,[this]{m_blotterModel->append(m_pendingEvents);m_pendingEvents.clear();});frameTimer->start();
    connect(m_gateway, &GatewayClient::marketFrameReceived, this, [this](const QList<QJsonObject> &events, quint64 conflated) {
        for (const auto &event : events) appendEvent(event);
        if (conflated) statusBar()->showMessage(QStringLiteral("本帧合并 %1 条行情更新").arg(conflated), 1500);
    });
    connect(m_gateway, &GatewayClient::sequenceGap, this, [this](const QString &stream, quint64 expected, quint64 received) {
        statusBar()->showMessage(QStringLiteral("%1 事件序号缺口：期望 %2，收到 %3；正在请求恢复")
                                 .arg(stream).arg(expected).arg(received));
    });
    connect(m_gateway, &GatewayClient::requestFailed, this, [this](const QString &operation, const QString &detail) {
        statusBar()->showMessage(QStringLiteral("%1：%2").arg(operation, detail));
    });
    connect(m_context, &TerminalContext::contextChanged, this, [this] {
        m_contextLabel->setText(QStringLiteral("账户 %1 · 策略组 %2 · 运行 %3 · 标的 %4")
            .arg(m_context->accountId().isEmpty() ? QStringLiteral("—") : m_context->accountId(),
                 m_context->strategyGroupId().isEmpty() ? QStringLiteral("—") : m_context->strategyGroupId(),
                 m_context->runId().isEmpty() ? QStringLiteral("—") : m_context->runId(),
                 m_context->instrumentId().isEmpty() ? QStringLiteral("—") : m_context->instrumentId()));
    });

    KDDockWidgets::LayoutSaver saver;
    if (!saver.restoreFromFile(layoutPath())) activateWorkspace(QStringLiteral("实盘运行"));
    m_gateway->start();
}

MainWindow::~MainWindow() = default;

void MainWindow::createMenus()
{
    const QStringList workspaces = {
        QStringLiteral("实盘运行"), QStringLiteral("执行调查"), QStringLiteral("研究与发布"),
        QStringLiteral("组合与风险"), QStringLiteral("行情与数据"), QStringLiteral("运维与安全")
    };
    auto *workspaceMenu = menuBar()->addMenu(QStringLiteral("工作区"));
    for (const auto &workspace : workspaces) {
        auto *action = workspaceMenu->addAction(workspace);
        connect(action, &QAction::triggered, this, [this, workspace] { activateWorkspace(workspace); });
    }
    m_panelMenu = menuBar()->addMenu(QStringLiteral("面板"));
    connect(m_palette, &CommandPalette::commandSelected, this, [this](const QString &command) {
        if (m_panels.contains(command)) m_panels.value(command)->show();
        else activateWorkspace(command);
    });
    m_palette->setCommands(workspaces);
    auto *command = new QAction(QStringLiteral("命令搜索"), this);
    command->setShortcut(QKeySequence(QStringLiteral("Ctrl+K")));
    connect(command, &QAction::triggered, m_palette, &CommandPalette::openPalette);
    menuBar()->addAction(command);
}

void MainWindow::createPanels()
{
    auto *market = marketPanel();
    setPersistentCentralWidget(market);

    auto *universe = addPanel(QStringLiteral("策略标的"), QStringLiteral("策略标的"),
        tablePanel({QStringLiteral("品种"), QStringLiteral("板块"), QStringLiteral("目标权重"), QStringLiteral("实际权重"),
                    QStringLiteral("偏差"), QStringLiteral("持仓"), QStringLiteral("PnL")},
                   QStringLiteral("等待所选运行的策略标的快照")), KDDockWidgets::Location_OnLeft);
    auto *metrics = addPanel(QStringLiteral("收益与执行"), QStringLiteral("收益与执行"), metricPanel(),
                             KDDockWidgets::Location_OnTop);
    addPanel(QStringLiteral("策略控制"), QStringLiteral("策略控制"), new StrategyControlPanel(m_gateway, m_context),
             KDDockWidgets::Location_OnRight, metrics);

    auto *blotter = new QTableView;
    blotter->setModel(m_blotterModel);
    blotter->setSelectionBehavior(QAbstractItemView::SelectRows);
    blotter->setAlternatingRowColors(true);
    blotter->horizontalHeader()->setStretchLastSection(true);
    addPanel(QStringLiteral("事件与订单"), QStringLiteral("事件 / Order / Route / Fill / Risk / Inference"), blotter,
             KDDockWidgets::Location_OnBottom, universe);

    addPanel(QStringLiteral("生命周期"), QStringLiteral("执行生命周期"),
        tablePanel({QStringLiteral("发生时间"), QStringLiteral("阶段"), QStringLiteral("耗时"), QStringLiteral("订单号"),
                    QStringLiteral("Route"), QStringLiteral("成交号"), QStringLiteral("结果")},
                   QStringLiteral("选择订单或成交后显示完整关联链")), KDDockWidgets::Location_OnRight);
    addPanel(QStringLiteral("研究证据"), QStringLiteral("研究证据"),
        tablePanel({QStringLiteral("制品"), QStringLiteral("数据版本"), QStringLiteral("样本外区间"), QStringLiteral("交易数"),
                    QStringLiteral("PnL"), QStringLiteral("回撤"), QStringLiteral("Sharpe"), QStringLiteral("推理 P95")},
                   QStringLiteral("等待模型与 Alpha 评估目录")), KDDockWidgets::Location_OnRight);
    addPanel(QStringLiteral("组合归因"), QStringLiteral("组合归因"),
        tablePanel({QStringLiteral("品种 / 板块 / Alpha"), QStringLiteral("已实现"), QStringLiteral("未实现"),
                    QStringLiteral("费用"), QStringLiteral("滑点"), QStringLiteral("贡献")},
                   QStringLiteral("等待组合归因查询")), KDDockWidgets::Location_OnRight);
    addPanel(QStringLiteral("数据质量"), QStringLiteral("行情与数据质量"),
        tablePanel({QStringLiteral("来源"), QStringLiteral("最后更新"), QStringLiteral("延迟"), QStringLiteral("序号"),
                    QStringLiteral("合并"), QStringLiteral("缺口")}, QStringLiteral("等待数据质量指标")),
             KDDockWidgets::Location_OnRight);
    addPanel(QStringLiteral("服务状态"), QStringLiteral("运维与安全"),
        tablePanel({QStringLiteral("服务"), QStringLiteral("版本"), QStringLiteral("状态"), QStringLiteral("心跳"),
                    QStringLiteral("积压"), QStringLiteral("P99")}, QStringLiteral("等待服务状态快照")),
             KDDockWidgets::Location_OnRight);
    addPanel(QStringLiteral("接口与手动委托"),QStringLiteral("接口与手动委托"),new ApiWorkbench(m_gateway),KDDockWidgets::Location_OnBottom);

    QStringList commands = {QStringLiteral("实盘运行"), QStringLiteral("执行调查"), QStringLiteral("研究与发布"),
                            QStringLiteral("组合与风险"), QStringLiteral("行情与数据"), QStringLiteral("运维与安全")};
    for (auto it = m_panels.cbegin(); it != m_panels.cend(); ++it) commands.append(it.key());
    m_palette->setCommands(commands);
}

KDDockWidgets::QtWidgets::DockWidget *MainWindow::addPanel(const QString &id, const QString &title,
    QWidget *content, KDDockWidgets::Location location, KDDockWidgets::QtWidgets::DockWidget *relative)
{
    auto *dock = new KDDockWidgets::QtWidgets::DockWidget(id);
    dock->setTitle(title);
    dock->setWidget(content);
    addDockWidget(dock, location, relative);
    m_panels.insert(id, dock);
    m_panelMenu->addAction(dock->toggleAction());
    return dock;
}

QWidget *MainWindow::tablePanel(const QStringList &headers, const QString &emptyText)
{
    auto *container = new QWidget;
    auto *layout = new QVBoxLayout(container);
    layout->setContentsMargins(6, 6, 6, 6);
    auto *table = new QTableView(container);
    auto *model = new ArrowTableModel(table);
    model->clear(headers);
    table->setModel(model);
    table->setSortingEnabled(true);
    table->setAlternatingRowColors(true);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->horizontalHeader()->setStretchLastSection(true);
    layout->addWidget(table);
    auto *empty = new QLabel(emptyText, container);
    empty->setObjectName(QStringLiteral("emptyState"));
    layout->addWidget(empty);
    return container;
}

QWidget *MainWindow::metricPanel()
{
    auto *container = new QWidget;
    auto *form = new QFormLayout(container);
    form->setContentsMargins(10, 8, 10, 8);
    form->addRow(QStringLiteral("NAV / USDT"), valueLabel());
    form->addRow(QStringLiteral("PnL / USDT"), valueLabel());
    form->addRow(QStringLiteral("最大回撤 / %"), valueLabel());
    form->addRow(QStringLiteral("总敞口 / USDT"), valueLabel());
    form->addRow(QStringLiteral("工作委托"), valueLabel());
    form->addRow(QStringLiteral("报单 / 撤单 / 成交"), valueLabel());
    form->addRow(QStringLiteral("延迟 P50 / P95"), valueLabel());
    return container;
}

QWidget *MainWindow::marketPanel()
{
    auto *container = new QWidget;
    auto *layout = new QVBoxLayout(container);
    layout->setContentsMargins(6, 6, 6, 6);
    auto *toolbar = new QToolBar(container);
    toolbar->setMovable(false);
    toolbar->addWidget(new QLabel(QStringLiteral("行情 · 目标/实际仓位"), toolbar));
    layout->addWidget(toolbar);
    auto *chart = new QChart;
    chart->setTitle(QStringLiteral("等待所选标的的初始快照"));
    chart->legend()->hide();
    auto *view = new QChartView(chart, container);
    view->setRenderHint(QPainter::Antialiasing);
    layout->addWidget(view, 1);
    return container;
}

void MainWindow::activateWorkspace(const QString &workspace)
{
    static const QHash<QString, QStringList> visible = {
        {QStringLiteral("实盘运行"), {QStringLiteral("策略标的"), QStringLiteral("收益与执行"), QStringLiteral("策略控制"), QStringLiteral("事件与订单")}},
        {QStringLiteral("执行调查"), {QStringLiteral("生命周期"), QStringLiteral("事件与订单"), QStringLiteral("数据质量")}},
        {QStringLiteral("研究与发布"), {QStringLiteral("研究证据"), QStringLiteral("事件与订单")}},
        {QStringLiteral("组合与风险"), {QStringLiteral("组合归因"), QStringLiteral("收益与执行"), QStringLiteral("策略标的")}},
        {QStringLiteral("行情与数据"), {QStringLiteral("数据质量"), QStringLiteral("事件与订单")}},
        {QStringLiteral("运维与安全"), {QStringLiteral("服务状态"), QStringLiteral("数据质量"), QStringLiteral("事件与订单")}}
    };
    const auto selected = visible.value(workspace, visible.value(QStringLiteral("实盘运行")));
    for (auto it = m_panels.begin(); it != m_panels.end(); ++it) {
        if (selected.contains(it.key())) it.value()->show();
        else it.value()->close();
    }
    setWindowTitle(QStringLiteral("Montlok Terminal · %1").arg(workspace));
}

void MainWindow::applyBootstrap(const QJsonObject &bootstrap)
{
    const auto catalog = bootstrap.value(QStringLiteral("catalog")).toObject();
    const auto accounts = catalog.value(QStringLiteral("accounts")).toArray();
    const auto groups = catalog.value(QStringLiteral("strategy_groups")).toArray();
    const auto runs = catalog.value(QStringLiteral("runs")).toArray();
    if (!accounts.isEmpty()) m_context->setAccountId(accounts.first().toObject().value(QStringLiteral("id")).toString());
    if (!groups.isEmpty()) m_context->setStrategyGroupId(groups.first().toObject().value(QStringLiteral("id")).toString());
    if (!runs.isEmpty()) {
        const auto run = runs.first().toObject();
        m_context->setRunId(run.value(QStringLiteral("id")).toString());
        m_context->setInstrumentId(run.value(QStringLiteral("instrument_id")).toString());
        m_context->setModelReleaseId(run.value(QStringLiteral("model_release_id")).toString());
        m_context->setSignalVersion(run.value(QStringLiteral("signal_version")).toString());
    }
    m_connection->setText(QStringLiteral("目录已同步 · 协议 v%1").arg(bootstrap.value(QStringLiteral("protocol_version")).toInt()));
}

void MainWindow::appendEvent(const QJsonObject &event)
{
    m_pendingEvents.append(event);
}

QString MainWindow::layoutPath() const
{
    const auto directory = QStandardPaths::writableLocation(QStandardPaths::AppConfigLocation);
    QDir().mkpath(directory);
    return directory + QStringLiteral("/layout-v2.json");
}

void MainWindow::closeEvent(QCloseEvent *event)
{
    KDDockWidgets::LayoutSaver saver;
    if (!saver.saveToFile(layoutPath())) {
        statusBar()->showMessage(QStringLiteral("工作区布局未能保存"));
    }
    KDDockWidgets::QtWidgets::MainWindow::closeEvent(event);
}
