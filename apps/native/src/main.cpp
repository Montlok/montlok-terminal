#include "MainWindow.h"

#include <kddockwidgets/KDDockWidgets.h>

#include <QApplication>
#include <QCommandLineOption>
#include <QCommandLineParser>
#include <QStyleFactory>

int main(int argc, char **argv)
{
    QApplication application(argc, argv);
    QCoreApplication::setOrganizationName(QStringLiteral("Montlok"));
    QCoreApplication::setApplicationName(QStringLiteral("Montlok Terminal"));
    QCoreApplication::setApplicationVersion(QStringLiteral(MONTLOK_VERSION));
    application.setStyle(QStyleFactory::create(QStringLiteral("Fusion")));
    application.setStyleSheet(QStringLiteral(R"(
        QMainWindow, QWidget { background: #0B0F14; color: #E8EDF3; font-family: Inter, "PingFang SC", "Segoe UI"; }
        QMenuBar, QMenu, QToolBar, QStatusBar { background: #10161D; border-color: #222D39; }
        QMenuBar::item:selected, QMenu::item:selected { background: #142B46; }
        QTableView { background: #10161D; alternate-background-color: #0E141B; gridline-color: #222D39; selection-background-color: #142B46; }
        QHeaderView::section { background: #151D26; color: #9AA7B5; border: 0; border-right: 1px solid #222D39; padding: 5px; }
        QPushButton { background: #151D26; border: 1px solid #344353; border-radius: 3px; padding: 6px 10px; }
        QPushButton:hover { border-color: #4498F7; }
        QPushButton[operationAction="stop"] { color: #F05B65; }
        QLabel[numeric="true"] { font-family: "IBM Plex Mono", "SFMono-Regular", Consolas; }
        QLabel#emptyState { color: #687685; padding: 8px; }
        QLineEdit, QComboBox { background: #10161D; border: 1px solid #344353; padding: 5px; }
    )"));

    KDDockWidgets::initFrontend(KDDockWidgets::FrontendType::QtWidgets);
    QCommandLineParser parser;
    parser.setApplicationDescription(QStringLiteral("Montlok institutional trading terminal"));
    parser.addHelpOption();
    parser.addVersionOption();
    QCommandLineOption gateway(QStringList{QStringLiteral("g"), QStringLiteral("gateway")},
                               QStringLiteral("Gateway base URL"), QStringLiteral("url"),
                               QStringLiteral("https://tokyo.montlok.com"));
    parser.addOption(gateway);
    parser.process(application);

    MainWindow window(QUrl(parser.value(gateway)));
    window.show();
    return application.exec();
}
