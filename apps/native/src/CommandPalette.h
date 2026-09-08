#pragma once

#include <QDialog>
#include <QStringList>

class QLineEdit;
class QListWidget;

class CommandPalette final : public QDialog
{
    Q_OBJECT
public:
    explicit CommandPalette(QWidget *parent = nullptr);
    void setCommands(const QStringList &commands);
    void openPalette();

Q_SIGNALS:
    void commandSelected(const QString &command);

private:
    void filter(const QString &text);
    QStringList m_commands;
    QLineEdit *m_input;
    QListWidget *m_list;
};
