#include "GatewayClient.h"

#include "TerminalContext.h"
#include "DeviceIdentity.h"
#include "terminal.pb.h"
#include <google/protobuf/util/json_util.h>
#ifdef MONTLOK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#endif

#include <QJsonArray>
#include <QJsonDocument>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QUuid>
#include <QCryptographicHash>
#include <QDateTime>
#include <QDesktopServices>
#include <QSysInfo>
#include <qtkeychain/keychain.h>

namespace {
QJsonObject parseObject(const QByteArray &bytes)
{
    const auto document = QJsonDocument::fromJson(bytes);
    return document.isObject() ? document.object() : QJsonObject{};
}

QJsonObject envelopeJson(const montlok::v2::EventEnvelope &event)
{
    std::string json;
    const auto status=google::protobuf::util::MessageToJsonString(event,&json);
    if(!status.ok())return {};
    return parseObject(QByteArray::fromStdString(json));
}
}

GatewayClient::GatewayClient(QUrl baseUrl, TerminalContext *context, QObject *parent)
    : QObject(parent), m_baseUrl(std::move(baseUrl)), m_context(context),m_identity(new DeviceIdentity(this))
{
    connect(m_identity,&DeviceIdentity::ready,this,&GatewayClient::loadRefreshToken);
    connect(m_identity,&DeviceIdentity::failed,this,[this](const QString &message){Q_EMIT requestFailed(QStringLiteral("设备登录"),message);});
    m_tokenTimer.setSingleShot(true);
    connect(&m_tokenTimer,&QTimer::timeout,this,&GatewayClient::refreshAccessToken);
    m_authorizationTimer.setInterval(5000);
    connect(&m_authorizationTimer,&QTimer::timeout,this,&GatewayClient::pollAuthorization);
    m_frameTimer.setInterval(50);
    m_frameTimer.setTimerType(Qt::PreciseTimer);
    connect(&m_frameTimer, &QTimer::timeout, this, &GatewayClient::flushMarketFrames);
    connect(&m_socket, &QWebSocket::connected, this, [this] {
        Q_EMIT connectionStateChanged(QStringLiteral("实时流已连接"));
        sendHello();
        sendSubscription();
    });
    connect(&m_socket, &QWebSocket::disconnected, this, [this] {
        Q_EMIT connectionStateChanged(QStringLiteral("实时流已断开，正在恢复"));
        QTimer::singleShot(1000, this, &GatewayClient::connectStream);
    });
    connect(&m_socket, &QWebSocket::binaryMessageReceived, this, &GatewayClient::handleBinaryMessage);
    connect(&m_socket, &QWebSocket::errorOccurred, this, [this](QAbstractSocket::SocketError) {
        Q_EMIT requestFailed(QStringLiteral("stream"), m_socket.errorString());
    });
    // Batch a multi-field selection change into one new observation subscription.
    auto *selectionTimer=new QTimer(this);selectionTimer->setSingleShot(true);selectionTimer->setInterval(0);
    connect(context,&TerminalContext::contextChanged,this,[this,selectionTimer]{
        const auto scope=m_context->accountId()+QStringLiteral("/")+m_context->strategyGroupId()+QStringLiteral("/")+m_context->runId();
        if(scope==m_observationScope)return;m_observationScope=scope;selectionTimer->start();
    });
    connect(selectionTimer,&QTimer::timeout,this,[this]{if(m_socket.state()==QAbstractSocket::ConnectedState)sendSubscription();});
}

void GatewayClient::setBearerToken(const QByteArray &token) { m_bearerToken = token; }

void GatewayClient::loadRefreshToken()
{
    auto *job=new QKeychain::ReadPasswordJob(QStringLiteral("Montlok Terminal"),this);
    job->setKey(QStringLiteral("refresh/")+m_baseUrl.toString());
    connect(job,&QKeychain::Job::finished,this,[this,job]{
        if(job->error()==QKeychain::EntryNotFound){authorizeDevice();return;}
        if(job->error()){Q_EMIT requestFailed(QStringLiteral("设备登录"),job->errorString());return;}
        const auto saved=parseObject(job->textData().toUtf8());
        m_deviceId=saved.value(QStringLiteral("device_id")).toString();
        m_refreshToken=saved.value(QStringLiteral("refresh_token")).toString();
        if(m_deviceId.isEmpty()||m_refreshToken.isEmpty()){Q_EMIT requestFailed(QStringLiteral("设备登录"),QStringLiteral("保存的登录信息格式不正确"));return;}
        refreshAccessToken();
    });job->start();
}

void GatewayClient::authorizeDevice()
{
    QString platform;
#if defined(Q_OS_MACOS)
    platform=QStringLiteral("macos");
#elif defined(Q_OS_WIN)
    platform=QStringLiteral("windows");
#else
    platform=QStringLiteral("linux");
#endif
    const QJsonObject body{{QStringLiteral("device_name"),QSysInfo::machineHostName()},
        {QStringLiteral("platform"),platform},{QStringLiteral("public_key"),QString::fromLatin1(m_identity->publicKey().toBase64())}};
    auto *reply=m_network.post(requestFor(QStringLiteral("/api/v2/device/authorizations")),QJsonDocument(body).toJson(QJsonDocument::Compact));
    connect(reply,&QNetworkReply::finished,this,[this,reply]{
        const auto value=parseObject(reply->readAll());reply->deleteLater();
        if(reply->error()!=QNetworkReply::NoError){Q_EMIT requestFailed(QStringLiteral("设备登录"),value.value(QStringLiteral("error")).toString(reply->errorString()));return;}
        const QUrl approval(value.value(QStringLiteral("verification_uri")).toString());
        if(approval.host()!=m_baseUrl.host()||approval.scheme()!=m_baseUrl.scheme()){
            Q_EMIT requestFailed(QStringLiteral("设备登录"),QStringLiteral("设备授权地址与终端地址不一致"));return;
        }
        m_authorizationId=value.value(QStringLiteral("authorization_id")).toString();
        m_authorizationDeadline=QDateTime::currentSecsSinceEpoch()+value.value(QStringLiteral("expires_in")).toInteger();
        Q_EMIT connectionStateChanged(QStringLiteral("请在浏览器中批准设备 · %1").arg(value.value(QStringLiteral("user_code")).toString()));
        QDesktopServices::openUrl(approval);m_authorizationTimer.start();
    });
}

void GatewayClient::pollAuthorization()
{
    if(QDateTime::currentSecsSinceEpoch()>m_authorizationDeadline){m_authorizationTimer.stop();Q_EMIT requestFailed(QStringLiteral("设备登录"),QStringLiteral("设备授权已过期"));return;}
    const auto timestamp=QDateTime::currentSecsSinceEpoch();
    const auto material=QByteArray("device-authorize\n")+m_authorizationId.toUtf8()+'\n'+QByteArray::number(timestamp);
    const QJsonObject body{{QStringLiteral("authorization_id"),m_authorizationId},{QStringLiteral("timestamp"),timestamp},
        {QStringLiteral("signature"),QString::fromLatin1(m_identity->sign(material).toBase64())}};
    auto *reply=m_network.post(requestFor(QStringLiteral("/api/v2/device/tokens")),QJsonDocument(body).toJson(QJsonDocument::Compact));
    connect(reply,&QNetworkReply::finished,this,[this,reply]{
        const auto value=parseObject(reply->readAll());const auto status=reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();reply->deleteLater();
        if(status==202)return;
        m_authorizationTimer.stop();
        if(status==200)acceptTokens(value);
        else Q_EMIT requestFailed(QStringLiteral("设备登录"),value.value(QStringLiteral("error")).toString(reply->errorString()));
    });
}

void GatewayClient::refreshAccessToken()
{
    const auto timestamp=QDateTime::currentSecsSinceEpoch();const auto nonce=QUuid::createUuid().toString(QUuid::WithoutBraces);
    const auto material=QByteArray("device-refresh\n")+m_deviceId.toUtf8()+'\n'+nonce.toUtf8()+'\n'+
        QCryptographicHash::hash(m_refreshToken.toUtf8(),QCryptographicHash::Sha256).toHex()+'\n'+QByteArray::number(timestamp);
    const QJsonObject body{{QStringLiteral("device_id"),m_deviceId},{QStringLiteral("refresh_token"),m_refreshToken},
        {QStringLiteral("nonce"),nonce},{QStringLiteral("timestamp"),timestamp},{QStringLiteral("signature"),QString::fromLatin1(m_identity->sign(material).toBase64())}};
    auto *reply=m_network.post(requestFor(QStringLiteral("/api/v2/device/tokens/refresh")),QJsonDocument(body).toJson(QJsonDocument::Compact));
    connect(reply,&QNetworkReply::finished,this,[this,reply]{
        const auto value=parseObject(reply->readAll());reply->deleteLater();
        if(reply->error()==QNetworkReply::NoError)acceptTokens(value);
        else Q_EMIT requestFailed(QStringLiteral("设备登录"),value.value(QStringLiteral("error")).toString(reply->errorString()));
    });
}

void GatewayClient::acceptTokens(const QJsonObject &tokens)
{
    const auto device=tokens.value(QStringLiteral("device_id")).toString();
    const auto access=tokens.value(QStringLiteral("access_token")).toString();
    const auto refresh=tokens.value(QStringLiteral("refresh_token")).toString();
    if(device.isEmpty()||access.isEmpty()||refresh.isEmpty()){Q_EMIT requestFailed(QStringLiteral("设备登录"),QStringLiteral("设备授权响应不完整"));return;}
    auto *job=new QKeychain::WritePasswordJob(QStringLiteral("Montlok Terminal"),this);
    job->setKey(QStringLiteral("refresh/")+m_baseUrl.toString());
    job->setTextData(QString::fromUtf8(QJsonDocument(QJsonObject{{QStringLiteral("device_id"),device},{QStringLiteral("refresh_token"),refresh}}).toJson(QJsonDocument::Compact)));
    connect(job,&QKeychain::Job::finished,this,[this,job,device,access,refresh]{
        if(job->error()){Q_EMIT requestFailed(QStringLiteral("设备登录"),job->errorString());return;}
        m_deviceId=device;m_bearerToken=access.toUtf8();m_refreshToken=refresh;m_tokenTimer.start(270000);fetchBootstrap();
    });job->start();
}

QNetworkRequest GatewayClient::requestFor(const QString &path,const QByteArray &method,const QByteArray &body) const
{
    QNetworkRequest request(m_baseUrl.resolved(QUrl(path)));
    request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/json"));
    request.setTransferTimeout(15000);
    request.setRawHeader("Accept", "application/json");
    if (!m_bearerToken.isEmpty()) request.setRawHeader("Authorization", "Bearer " + m_bearerToken);
    if(!m_deviceId.isEmpty()) {
        const auto nonce=QUuid::createUuid().toString(QUuid::WithoutBraces);
        const auto timestamp=QString::number(QDateTime::currentSecsSinceEpoch());
        const auto material=QByteArray("v2\n")+m_deviceId.toUtf8()+'\n'+nonce.toUtf8()+'\n'+method+'\n'+path.toUtf8()+'\n'+QCryptographicHash::hash(body,QCryptographicHash::Sha256).toHex()+'\n'+timestamp.toUtf8();
        request.setRawHeader("X-Montlok-Device",m_deviceId.toUtf8());request.setRawHeader("X-Montlok-Nonce",nonce.toUtf8());
        request.setRawHeader("X-Montlok-Timestamp",timestamp.toUtf8());request.setRawHeader("X-Montlok-Signature",m_identity->sign(material).toBase64());
    }
    return request;
}

void GatewayClient::start()
{
    if(m_bearerToken.isEmpty()){m_identity->loadOrCreate();return;}
    fetchBootstrap();
}

void GatewayClient::fetchBootstrap()
{
    Q_EMIT connectionStateChanged(QStringLiteral("正在读取终端目录"));
    auto *reply = m_network.get(requestFor(QStringLiteral("/api/v2/bootstrap")));
    connect(reply, &QNetworkReply::finished, this, [this, reply] {
        const auto body = reply->readAll();
        if (reply->error() == QNetworkReply::NoError) {
            Q_EMIT bootstrapReceived(parseObject(body));
            connectStream();
            m_frameTimer.start();
        } else {
            Q_EMIT requestFailed(QStringLiteral("bootstrap"), reply->errorString());
        }
        reply->deleteLater();
    });
}

void GatewayClient::connectStream()
{
    if (m_socket.state() == QAbstractSocket::ConnectedState || m_socket.state() == QAbstractSocket::ConnectingState)
        return;
    auto url = m_baseUrl.resolved(QUrl(QStringLiteral("/api/v2/stream")));
    url.setScheme(url.scheme() == QStringLiteral("https") ? QStringLiteral("wss") : QStringLiteral("ws"));
    QNetworkRequest request=requestFor(QStringLiteral("/api/v2/stream"));request.setUrl(url);
    request.setRawHeader("Sec-WebSocket-Protocol", "montlok.protobuf.v2");
    if (!m_bearerToken.isEmpty()) request.setRawHeader("Authorization", "Bearer " + m_bearerToken);
    Q_EMIT connectionStateChanged(QStringLiteral("正在连接实时流"));
    m_socket.open(request);
}

void GatewayClient::sendHello()
{
    montlok::v2::ClientMessage message;
    auto *hello = message.mutable_hello();
    hello->set_protocol_version(2);
    hello->set_device_id(m_deviceId.toStdString());
    for (auto it = m_sequences.cbegin(); it != m_sequences.cend(); ++it) {
        auto *position = hello->add_resume_positions();
        position->set_stream(it.key().toStdString());
        position->set_last_stream_seq(it.value());
    }
    std::string encoded;
    if(!message.SerializeToString(&encoded))return;
    m_socket.sendBinaryMessage(QByteArray::fromStdString(encoded));
}

void GatewayClient::sendSubscription()
{
    montlok::v2::ClientMessage message;
    auto *subscription = message.mutable_subscribe();
    subscription->set_request_id(QUuid::createUuid().toString(QUuid::WithoutBraces).toStdString());
    if(m_context->runId().isEmpty())return;
    subscription->add_topics((QStringLiteral("run.")+m_context->runId()).toStdString());
    subscription->set_conflate_ms(50);
    auto *context = subscription->mutable_context();
    context->set_account_id(m_context->accountId().toStdString());
    context->set_strategy_group_id(m_context->strategyGroupId().toStdString());
    context->set_run_id(m_context->runId().toStdString());
    // The run stream includes all instruments so its sequence remains contiguous.
    // Per-instrument filtering is a view concern, not a reason to omit run events.
    context->set_model_release_id(m_context->modelReleaseId().toStdString());
    context->set_signal_version(m_context->signalVersion().toStdString());
    std::string encoded;
    if(!message.SerializeToString(&encoded))return;
    m_socket.sendBinaryMessage(QByteArray::fromStdString(encoded));
}

void GatewayClient::handleBinaryMessage(const QByteArray &bytes)
{
    montlok::v2::ServerMessage message;
    if (!message.ParseFromArray(bytes.constData(), bytes.size())) {
        Q_EMIT requestFailed(QStringLiteral("stream"), QStringLiteral("无法解析实时消息"));
        return;
    }
    if (message.has_event()) {
        const auto &event = message.event();
        const auto stream = QString::fromStdString(event.stream());
        const auto previous = m_sequences.value(stream, 0);
        if(event.stream_seq()<=previous)return;
        if (event.stream_seq() != previous + 1) {
            Q_EMIT sequenceGap(stream, previous + 1, event.stream_seq());
            m_socket.close();
            return;
        }
        m_sequences.insert(stream, event.stream_seq());
        const bool market = event.event_type() == montlok::v2::MARKET_QUOTE ||
                            event.event_type() == montlok::v2::ORDER_BOOK_UPDATED;
        acceptEvent(envelopeJson(event), market);
    } else if(message.has_snapshot_begin()) {
        const auto &begin=message.snapshot_begin();m_snapshotId=QString::fromStdString(begin.subscription_id());
        m_snapshotEvents.clear();m_snapshotPositions.clear();
        for(const auto &position:begin.watermarks())m_snapshotPositions.insert(QString::fromStdString(position.stream()),position.last_stream_seq());
        Q_EMIT connectionStateChanged(QStringLiteral("正在同步运行快照"));
    } else if(message.has_snapshot_batch()) {
        const auto &batch=message.snapshot_batch();
        if(QString::fromStdString(batch.subscription_id())!=m_snapshotId || batch.arrow_ipc().size()>1048576 || batch.row_count()>2048){m_socket.close();return;}
#ifdef MONTLOK_HAS_ARROW
        auto buffer=arrow::Buffer::FromString(batch.arrow_ipc());
        auto input=std::make_shared<arrow::io::BufferReader>(buffer);
        auto readerResult=arrow::ipc::RecordBatchStreamReader::Open(input);
        if(!readerResult.ok()){m_socket.close();return;}
        auto reader=readerResult.ValueOrDie();int rows=0;
        while(true){auto result=reader->Next();if(!result.ok()){m_socket.close();return;}auto record=result.ValueOrDie();if(!record)break;
            auto column=record->GetColumnByName("payload_protobuf");
            if(!column || column->type_id()!=arrow::Type::BINARY){m_socket.close();return;}
            auto payloads=std::static_pointer_cast<arrow::BinaryArray>(column);
            for(int64_t index=0;index<payloads->length();++index){const auto bytes=payloads->GetView(index);montlok::v2::EventEnvelope event;
                if(!event.ParseFromArray(bytes.data(),static_cast<int>(bytes.size()))){m_socket.close();return;}
                if(event.stream_seq()>m_snapshotPositions.value(QString::fromStdString(event.stream()))){m_socket.close();return;}
                m_snapshotEvents.append(envelopeJson(event));++rows;
            }
        }
        if(rows!=static_cast<int>(batch.row_count())){m_socket.close();return;}
#else
        Q_EMIT requestFailed(QStringLiteral("快照"),QStringLiteral("此构建缺少 Arrow 数据解析组件"));m_socket.close();return;
#endif
    } else if(message.has_snapshot_end()) {
        const auto &end=message.snapshot_end();
        if(!m_snapshotId.isEmpty()){
            if(QString::fromStdString(end.subscription_id())!=m_snapshotId){m_socket.close();return;}
            for(const auto &position:end.watermarks())if(m_snapshotPositions.value(QString::fromStdString(position.stream()))!=position.last_stream_seq()){m_socket.close();return;}
            m_sequences=m_snapshotPositions;Q_EMIT initialSnapshotReceived(m_snapshotEvents);m_snapshotEvents.clear();m_snapshotId.clear();
        }
        Q_EMIT connectionStateChanged(QStringLiteral("运行事件已同步"));
    } else if (message.has_sequence_gap()) {
        const auto &gap = message.sequence_gap();
        Q_EMIT sequenceGap(QString::fromStdString(gap.stream()), gap.expected(), gap.received());
        m_socket.close();
    } else if (message.has_error()) {
        Q_EMIT requestFailed(QStringLiteral("stream"), QString::fromStdString(message.error().detail()));
        if(message.error().retryable())m_socket.close();
    }
}

void GatewayClient::acceptEvent(const QJsonObject &event, bool conflate)
{
    if (!conflate) {
        Q_EMIT eventReceived(event);
        return;
    }
    const auto key = event.value(QStringLiteral("instrumentId")).toString() + QStringLiteral(":") +
                     event.value(QStringLiteral("eventType")).toString();
    if (m_pendingMarketEvents.contains(key)) ++m_conflatedCount;
    m_pendingMarketEvents.insert(key, event);
}

void GatewayClient::flushMarketFrames()
{
    if (m_pendingMarketEvents.isEmpty()) return;
    Q_EMIT marketFrameReceived(m_pendingMarketEvents.values(), m_conflatedCount);
    m_pendingMarketEvents.clear();
    m_conflatedCount = 0;
}

QJsonObject GatewayClient::contextJson() const
{
    return {
        {QStringLiteral("account_id"), m_context->accountId()},
        {QStringLiteral("strategy_group_id"), m_context->strategyGroupId()},
        {QStringLiteral("run_id"), m_context->runId()},
        {QStringLiteral("instrument_id"), m_context->instrumentId()},
        {QStringLiteral("model_release_id"), m_context->modelReleaseId()},
        {QStringLiteral("signal_version"), m_context->signalVersion()}
    };
}

void GatewayClient::prepareOperation(const QString &action, const QJsonObject &parameters)
{
    postJson(QStringLiteral("/api/v2/operations/prepare"), {
        {QStringLiteral("action"), action},
        {QStringLiteral("context"), contextJson()},
        {QStringLiteral("parameters"), parameters}
    }, QStringLiteral("prepare"));
}

void GatewayClient::executeOperation(const QString &operationId, const QString &confirmationHash)
{
    postJson(QStringLiteral("/api/v2/operations/%1/execute").arg(operationId),
             {{QStringLiteral("confirmation_hash"), confirmationHash}}, QStringLiteral("execute"));
}

void GatewayClient::queryOperation(const QString &operationId)
{
    auto *reply = m_network.get(requestFor(QStringLiteral("/api/v2/operations/%1").arg(operationId)));
    connect(reply, &QNetworkReply::finished, this, [this, reply] {
        const auto body = reply->readAll();
        if (reply->error() == QNetworkReply::NoError) Q_EMIT operationReceipt(parseObject(body));
        else Q_EMIT requestFailed(QStringLiteral("receipt"), parseObject(body).value(QStringLiteral("error")).toString(reply->errorString()));
        reply->deleteLater();
    });
}

void GatewayClient::getLegacy(const QString &path,const QString &tag)
{
    auto *reply=m_network.get(requestFor(QStringLiteral("/api/v2/legacy/")+path));
    connect(reply,&QNetworkReply::finished,this,[this,reply,tag]{
        const auto document=QJsonDocument::fromJson(reply->readAll());
        const QJsonValue value=document.isArray()?QJsonValue(document.array()):QJsonValue(document.object());
        if(reply->error()==QNetworkReply::NoError)Q_EMIT datasetReceived(tag,value);
        else Q_EMIT requestFailed(tag,value.toObject().value(QStringLiteral("error")).toString(reply->errorString()));
        reply->deleteLater();
    });
}

void GatewayClient::queryRelated(const QString &eventId)
{
    const auto path=QStringLiteral("/api/v2/events/")+QString::fromLatin1(QUrl::toPercentEncoding(eventId))+QStringLiteral("/related?limit=500");
    auto *reply=m_network.get(requestFor(path));
    connect(reply,&QNetworkReply::finished,this,[this,reply,eventId]{
        const auto object=parseObject(reply->readAll());
        if(reply->error()!=QNetworkReply::NoError){Q_EMIT requestFailed(QStringLiteral("detail/")+eventId,reply->errorString());reply->deleteLater();return;}
        QList<QJsonObject> events;
        const auto encoded=object.value(QStringLiteral("events")).toArray();
        bool valid=object.value(QStringLiteral("encoding")).toString()==QStringLiteral("protobuf-base64")&&encoded.size()<=2048;
        for(const auto &value:encoded){
            const auto bytes=QByteArray::fromBase64(value.toString().toLatin1());montlok::v2::EventEnvelope event;
            if(!event.ParseFromArray(bytes.constData(),bytes.size())){valid=false;break;}
            events.append(envelopeJson(event));
        }
        if(valid)Q_EMIT relatedReceived(eventId,events,object.value(QStringLiteral("truncated")).toBool());
        else Q_EMIT requestFailed(QStringLiteral("detail/")+eventId,QStringLiteral("关联事件响应格式不完整"));
        reply->deleteLater();
    });
}

void GatewayClient::postLegacy(const QString &path,const QJsonObject &body,const QString &tag)
{
    const auto bytes=QJsonDocument(body).toJson(QJsonDocument::Compact);
    auto request=requestFor(QStringLiteral("/api/v2/legacy/")+path,"POST",bytes);request.setTransferTimeout(65000);
    auto *reply=m_network.post(request,bytes);
    connect(reply,&QNetworkReply::finished,this,[this,reply,tag]{
        const auto document=QJsonDocument::fromJson(reply->readAll());
        const QJsonValue value=document.isArray()?QJsonValue(document.array()):QJsonValue(document.object());
        if(reply->error()==QNetworkReply::NoError)Q_EMIT datasetReceived(tag,value);
        else Q_EMIT requestFailed(tag,value.toObject().value(QStringLiteral("error")).toString(reply->errorString()));
        reply->deleteLater();
    });
}

void GatewayClient::postJson(const QString &path, const QJsonObject &body, const QString &kind)
{
    const auto bytes=QJsonDocument(body).toJson(QJsonDocument::Compact);
    auto *reply = m_network.post(requestFor(path,"POST",bytes), bytes);
    connect(reply, &QNetworkReply::finished, this, [this, reply, kind] {
        const auto body = reply->readAll();
        const auto value = parseObject(body);
        if (reply->error() == QNetworkReply::NoError) {
            if (kind == QStringLiteral("prepare")) Q_EMIT operationPrepared(value);
            else Q_EMIT operationReceipt(value);
        } else {
            Q_EMIT requestFailed(kind, value.value(QStringLiteral("error")).toString(reply->errorString()));
        }
        reply->deleteLater();
    });
}
