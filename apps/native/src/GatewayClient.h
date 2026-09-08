#pragma once

#include <QHash>
#include <QJsonObject>
#include <QNetworkAccessManager>
#include <QObject>
#include <QQueue>
#include <QTimer>
#include <QUrl>
#include <QWebSocket>

class TerminalContext;
class DeviceIdentity;

class GatewayClient final : public QObject
{
    Q_OBJECT
public:
    explicit GatewayClient(QUrl baseUrl, TerminalContext *context, QObject *parent = nullptr);
    void setBearerToken(const QByteArray &token);
    void start();
    void prepareOperation(const QString &action, const QJsonObject &parameters = {});
    void executeOperation(const QString &operationId, const QString &confirmationHash);
    void queryOperation(const QString &operationId);
    void getLegacy(const QString &path,const QString &tag);
    void queryRelated(const QString &eventId);
    void postLegacy(const QString &path,const QJsonObject &body,const QString &tag);

Q_SIGNALS:
    void connectionStateChanged(const QString &state);
    void bootstrapReceived(const QJsonObject &bootstrap);
    void eventReceived(const QJsonObject &event);
    void initialSnapshotReceived(const QList<QJsonObject> &events);
    void marketFrameReceived(const QList<QJsonObject> &events, quint64 conflatedCount);
    void sequenceGap(const QString &stream, quint64 expected, quint64 received);
    void operationPrepared(const QJsonObject &operation);
    void operationReceipt(const QJsonObject &receipt);
    void requestFailed(const QString &operation, const QString &detail);
    void datasetReceived(const QString &tag,const QJsonValue &value);
    void relatedReceived(const QString &eventId,const QList<QJsonObject> &events,bool truncated);

private:
    QNetworkRequest requestFor(const QString &path, const QByteArray &method="GET", const QByteArray &body={}) const;
    void fetchBootstrap();
    void loadRefreshToken();
    void authorizeDevice();
    void pollAuthorization();
    void refreshAccessToken();
    void acceptTokens(const QJsonObject &tokens);
    void connectStream();
    void sendHello();
    void sendSubscription();
    void handleBinaryMessage(const QByteArray &message);
    void acceptEvent(const QJsonObject &event, bool conflate);
    void flushMarketFrames();
    QJsonObject contextJson() const;
    void postJson(const QString &path, const QJsonObject &body, const QString &kind);

    QUrl m_baseUrl;
    TerminalContext *m_context;
    QNetworkAccessManager m_network;
    QWebSocket m_socket;
    QByteArray m_bearerToken;
    DeviceIdentity *m_identity;
    QString m_deviceId;
    QString m_refreshToken;
    QString m_authorizationId;
    qint64 m_authorizationDeadline=0;
    QTimer m_tokenTimer;
    QTimer m_authorizationTimer;
    QHash<QString, quint64> m_sequences;
    QString m_snapshotId;
    QList<QJsonObject> m_snapshotEvents;
    QHash<QString,quint64> m_snapshotPositions;
    QHash<QString, QJsonObject> m_pendingMarketEvents;
    quint64 m_conflatedCount = 0;
    QTimer m_frameTimer;
    QString m_observationScope;
};
