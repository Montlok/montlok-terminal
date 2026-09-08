#include "DeviceIdentity.h"

#include <qtkeychain/keychain.h>
#include <sodium.h>

DeviceIdentity::DeviceIdentity(QObject *parent) : QObject(parent)
{
    if (sodium_init() < 0)
        Q_EMIT failed(QStringLiteral("设备签名库初始化失败"));
}
DeviceIdentity::~DeviceIdentity(){if(!m_secretKey.isEmpty())sodium_memzero(m_secretKey.data(),static_cast<size_t>(m_secretKey.size()));}

void DeviceIdentity::loadOrCreate()
{
    auto *job = new QKeychain::ReadPasswordJob(QStringLiteral("Montlok Terminal"), this);
    job->setKey(QStringLiteral("device-ed25519-secret"));
    connect(job, &QKeychain::Job::finished, this, [this, job] {
        if (!job->error()) {
            m_secretKey = QByteArray::fromBase64(job->textData().toLatin1());
            if (m_secretKey.size() == crypto_sign_SECRETKEYBYTES) {
                m_publicKey = m_secretKey.mid(crypto_sign_SECRETKEYBYTES - crypto_sign_PUBLICKEYBYTES);
                Q_EMIT ready();
                return;
            }
            Q_EMIT failed(QStringLiteral("钥匙串中的设备密钥格式不正确"));
            return;
        } else if (job->error()!=QKeychain::EntryNotFound) {
            Q_EMIT failed(job->errorString());
            return;
        }
        m_publicKey.resize(crypto_sign_PUBLICKEYBYTES);
        m_secretKey.resize(crypto_sign_SECRETKEYBYTES);
        if (crypto_sign_keypair(reinterpret_cast<unsigned char *>(m_publicKey.data()),
                                reinterpret_cast<unsigned char *>(m_secretKey.data())) != 0) {
            Q_EMIT failed(QStringLiteral("无法生成设备密钥"));
            return;
        }
        persistSecret();
    });
    job->start();
}

void DeviceIdentity::persistSecret()
{
    auto *job = new QKeychain::WritePasswordJob(QStringLiteral("Montlok Terminal"), this);
    job->setKey(QStringLiteral("device-ed25519-secret"));
    job->setTextData(QString::fromLatin1(m_secretKey.toBase64()));
    connect(job, &QKeychain::Job::finished, this, [this, job] {
        if (job->error()) Q_EMIT failed(job->errorString());
        else Q_EMIT ready();
    });
    job->start();
}

QByteArray DeviceIdentity::publicKey() const { return m_publicKey; }

QByteArray DeviceIdentity::sign(const QByteArray &message) const
{
    if (m_secretKey.size() != crypto_sign_SECRETKEYBYTES) return {};
    QByteArray signature(crypto_sign_BYTES, Qt::Uninitialized);
    crypto_sign_detached(reinterpret_cast<unsigned char *>(signature.data()), nullptr,
                         reinterpret_cast<const unsigned char *>(message.constData()),
                         static_cast<unsigned long long>(message.size()),
                         reinterpret_cast<const unsigned char *>(m_secretKey.constData()));
    return signature;
}
