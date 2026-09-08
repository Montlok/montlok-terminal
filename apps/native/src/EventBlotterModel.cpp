#include "EventBlotterModel.h"
#include "EventPresentation.h"
#include <QDateTime>
#include <QTimeZone>

QVariant EventBlotterModel::data(const QModelIndex &index,int role)const{
    if(!index.isValid()||index.row()>=m_events.size()||role!=Qt::DisplayRole)return {};
    const auto &event=m_events.at(index.row());
    switch(index.column()){
    case 0:return exactEventTime(event.value(QStringLiteral("occurredAtNs")).toVariant().toLongLong());
    case 1:return eventTypeLabel(event.value(QStringLiteral("eventType")).toString());
    case 2:return event.value(QStringLiteral("stream")).toString()+QStringLiteral(" / ")+event.value(QStringLiteral("streamSeq")).toVariant().toString();
    case 3:return event.value(QStringLiteral("instrumentId")).toString();
    case 4:return event.value(QStringLiteral("correlationId")).toString();
    case 5:return event.value(QStringLiteral("source")).toString();
    default:return {};
    }
}
QVariant EventBlotterModel::headerData(int section,Qt::Orientation orientation,int role)const{
    if(role!=Qt::DisplayRole)return {};if(orientation==Qt::Vertical)return section+1;
    const QStringList headers{QStringLiteral("时间"),QStringLiteral("类型"),QStringLiteral("流 / 序号"),QStringLiteral("标的"),QStringLiteral("关联编号"),QStringLiteral("来源")};
    return section>=0&&section<headers.size()?QVariant(headers.at(section)):QVariant();
}
void EventBlotterModel::append(const QList<QJsonObject> &events){
    if(events.isEmpty())return;
    beginInsertRows({},m_events.size(),m_events.size()+events.size()-1);m_events.append(events);endInsertRows();
    constexpr int maximum=20'000;
    if(m_events.size()>maximum){const int remove=m_events.size()-maximum;beginRemoveRows({},0,remove-1);m_events.remove(0,remove);endRemoveRows();}
}
void EventBlotterModel::replace(const QList<QJsonObject> &events){beginResetModel();m_events=events.last(std::min<qsizetype>(events.size(),20'000));endResetModel();}
