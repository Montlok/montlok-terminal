#include "CommandPalette.h"

#include <QKeyEvent>
#include <QLineEdit>
#include <QListWidget>
#include <QVBoxLayout>

CommandPalette::CommandPalette(QWidget *parent)
    : QDialog(parent), m_input(new QLineEdit(this)), m_list(new QListWidget(this))
{
    setWindowTitle(QStringLiteral("命令"));
    setModal(true);
    resize(620, 420);
    auto *layout = new QVBoxLayout(this);
    m_input->setPlaceholderText(QStringLiteral("输入工作区、面板或操作名称"));
    layout->addWidget(m_input);
    layout->addWidget(m_list);
    connect(m_input, &QLineEdit::textChanged, this, &CommandPalette::filter);
    connect(m_input, &QLineEdit::returnPressed, this, [this] {
        if (auto *item = m_list->currentItem()) {
            Q_EMIT commandSelected(item->text());
            accept();
        }
    });
    connect(m_list, &QListWidget::itemActivated, this, [this](QListWidgetItem *item) {
        Q_EMIT commandSelected(item->text());
        accept();
    });
}

void CommandPalette::setCommands(const QStringList &commands)
{
    m_commands = commands;
    filter(QString());
}

void CommandPalette::openPalette()
{
    m_input->clear();
    m_input->setFocus();
    show();
    raise();
}

void CommandPalette::filter(const QString &text)
{
    m_list->clear();
    for (const auto &command : m_commands) {
        if (text.isEmpty() || command.contains(text, Qt::CaseInsensitive)) m_list->addItem(command);
    }
    if (m_list->count()) m_list->setCurrentRow(0);
}
