#pragma once

#include <kddockwidgets/MainWindow.h>

#include <QHash>
#include <QJsonObject>

namespace KDDockWidgets::QtWidgets { class DockWidget; }
class CommandPalette;
class GatewayClient;
class QLabel;
class QMenu;
class EventBlotterModel;
class TerminalContext;

class MainWindow final : public KDDockWidgets::QtWidgets::MainWindow
{
    Q_OBJECT
public:
    explicit MainWindow(const QUrl &gatewayUrl, QWidget *parent = nullptr);
    ~MainWindow() override;

protected:
    void closeEvent(QCloseEvent *event) override;

private:
    KDDockWidgets::QtWidgets::DockWidget *addPanel(const QString &id, const QString &title,
                                                   QWidget *content, KDDockWidgets::Location location,
                                                   KDDockWidgets::QtWidgets::DockWidget *relative = nullptr);
    QWidget *tablePanel(const QStringList &headers, const QString &emptyText);
    QWidget *metricPanel();
    QWidget *marketPanel();
    void createMenus();
    void createPanels();
    void activateWorkspace(const QString &workspace);
    void applyBootstrap(const QJsonObject &bootstrap);
    void appendEvent(const QJsonObject &event);
    QString layoutPath() const;

    TerminalContext *m_context;
    GatewayClient *m_gateway;
    CommandPalette *m_palette;
    QLabel *m_connection;
    QLabel *m_contextLabel;
    QMenu *m_panelMenu;
    EventBlotterModel *m_blotterModel;
    QList<QJsonObject> m_pendingEvents;
    QHash<QString, KDDockWidgets::QtWidgets::DockWidget *> m_panels;
};
