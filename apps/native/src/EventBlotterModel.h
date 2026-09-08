#pragma once
#include <QAbstractTableModel>
#include <QJsonObject>
#include <QList>

class EventBlotterModel final:public QAbstractTableModel {
public:
    explicit EventBlotterModel(QObject *parent=nullptr):QAbstractTableModel(parent){}
    int rowCount(const QModelIndex &parent={})const override{return parent.isValid()?0:m_events.size();}
    int columnCount(const QModelIndex &parent={})const override{return parent.isValid()?0:6;}
    QVariant data(const QModelIndex &index,int role=Qt::DisplayRole)const override;
    QVariant headerData(int section,Qt::Orientation orientation,int role)const override;
    void append(const QList<QJsonObject> &events);
    void replace(const QList<QJsonObject> &events);
    QJsonObject eventAt(int row)const{return row>=0&&row<m_events.size()?m_events.at(row):QJsonObject{};}
private:
    QList<QJsonObject> m_events;
};
