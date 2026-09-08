#pragma once

#include <QAbstractTableModel>
#include <QStringList>
#include <memory>

#ifdef MONTLOK_HAS_ARROW
namespace arrow { class Table; }
#endif

class ArrowTableModel final : public QAbstractTableModel
{
    Q_OBJECT
public:
    explicit ArrowTableModel(QObject *parent = nullptr);
    int rowCount(const QModelIndex &parent = QModelIndex()) const override;
    int columnCount(const QModelIndex &parent = QModelIndex()) const override;
    QVariant data(const QModelIndex &index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role) const override;
    void clear(const QStringList &headers = {});

#ifdef MONTLOK_HAS_ARROW
    void setTable(std::shared_ptr<arrow::Table> table);
#endif

private:
    QStringList m_headers;
#ifdef MONTLOK_HAS_ARROW
    std::shared_ptr<arrow::Table> m_table;
#endif
};
