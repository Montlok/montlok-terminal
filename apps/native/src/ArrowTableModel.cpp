#include "ArrowTableModel.h"

#ifdef MONTLOK_HAS_ARROW
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#endif

ArrowTableModel::ArrowTableModel(QObject *parent) : QAbstractTableModel(parent) {}

int ArrowTableModel::rowCount(const QModelIndex &parent) const
{
    if (parent.isValid()) return 0;
#ifdef MONTLOK_HAS_ARROW
    return m_table ? static_cast<int>(m_table->num_rows()) : 0;
#else
    return 0;
#endif
}

int ArrowTableModel::columnCount(const QModelIndex &parent) const
{
    if (parent.isValid()) return 0;
#ifdef MONTLOK_HAS_ARROW
    return m_table ? static_cast<int>(m_table->num_columns()) : m_headers.size();
#else
    return m_headers.size();
#endif
}

QVariant ArrowTableModel::data(const QModelIndex &index, int role) const
{
    if (!index.isValid() || role != Qt::DisplayRole) return {};
#ifdef MONTLOK_HAS_ARROW
    if (!m_table || index.column() >= m_table->num_columns()) return {};
    const auto column = m_table->column(index.column());
    const auto chunkedIndex = static_cast<int64_t>(index.row());
    int64_t base = 0;
    for (const auto &chunk : column->chunks()) {
        if (chunkedIndex < base + chunk->length()) {
            const auto scalar = chunk->GetScalar(chunkedIndex - base);
            if (!scalar.ok() || !scalar.ValueOrDie()->is_valid) return {};
            return QString::fromStdString(scalar.ValueOrDie()->ToString());
        }
        base += chunk->length();
    }
#endif
    return {};
}

QVariant ArrowTableModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (role != Qt::DisplayRole) return {};
    if (orientation == Qt::Vertical) return section + 1;
#ifdef MONTLOK_HAS_ARROW
    if (m_table && section < m_table->num_columns())
        return QString::fromStdString(m_table->field(section)->name());
#endif
    return section < m_headers.size() ? QVariant(m_headers.at(section)) : QVariant();
}

void ArrowTableModel::clear(const QStringList &headers)
{
    beginResetModel();
    m_headers = headers;
#ifdef MONTLOK_HAS_ARROW
    m_table.reset();
#endif
    endResetModel();
}

#ifdef MONTLOK_HAS_ARROW
void ArrowTableModel::setTable(std::shared_ptr<arrow::Table> table)
{
    beginResetModel();
    m_headers.clear();
    m_table = std::move(table);
    endResetModel();
}
#endif
