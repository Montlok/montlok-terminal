#pragma once

#include <QObject>
#include <QByteArray>

class DeviceIdentity final : public QObject
{
    Q_OBJECT
public:
    explicit DeviceIdentity(QObject *parent = nullptr);
    ~DeviceIdentity() override;
    void loadOrCreate();
    QByteArray publicKey() const;
    QByteArray sign(const QByteArray &message) const;

Q_SIGNALS:
    void ready();
    void failed(const QString &detail);

private:
    void persistSecret();
    QByteArray m_publicKey;
    QByteArray m_secretKey;
};
