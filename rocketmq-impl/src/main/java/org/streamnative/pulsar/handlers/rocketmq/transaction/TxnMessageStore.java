package org.streamnative.pulsar.handlers.rocketmq.transaction;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_MESSAGE_ID;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopPutMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.CommitLogOffset;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

@Slf4j
public class TxnMessageStore {

    private final RocketMQBrokerController brokerController;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();

    private final SocketAddress storeHost;
    // 记录事务主题未处理的消息
    private final ConcurrentHashMap<Long, TxnMessage> txnMessages = new ConcurrentHashMap<>();

    private static final Cache<String, Producer<byte[]>> producers = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .initialCapacity(1024)
            .removalListener((RemovalListener<String, Producer<byte[]>>) listener -> {
                log.info("remove internal producer from caches [name={}].", listener.getKey());
                Producer<byte[]> producer = listener.getValue();
                if (producer != null) {
                    producer.closeAsync();
                }
            })
            .build();

    // 实时获取发送到事务主题中的消息
    private Consumer<byte[]> messageConsumer;
    // 实时获取发送到事务操作主题中的消息
    private Consumer<byte[]> opMessageConsumer;

    public TxnMessageStore(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        String brokerHost = RemotingUtil.getLocalAddress();;
        this.storeHost = new InetSocketAddress(brokerHost, brokerController.getBrokerPort());
    }

    public void start() {
        try {
            messageConsumer = createPulsarConsumer(TransactionalMessageUtil.buildHalfTopic());
            opMessageConsumer = createPulsarConsumer(TransactionalMessageUtil.buildOpTopic());
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner messageInner, final boolean opMessage) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("method {}, opMessage {}, data {}", "putMessage", opMessage, messageInner);
            }

            long startTimeMs = System.currentTimeMillis();
            final String msgId = messageInner.getProperties().get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            final String msgKey = messageInner.getProperties().get(PROPERTY_KEYS);
            final String msgTag = messageInner.getProperties().get(PROPERTY_TAGS);
            byte[] body = this.entryFormatter.encode(messageInner);
            int partitionId = 0; // TODO

            RocketMQTopic rmqTopic = new RocketMQTopic(messageInner.getTopic());
            final String pTopic = rmqTopic.getPartitionName(partitionId);
            CompletableFuture<MessageId> messageIdFuture = getProducerFromCache(pTopic)
                    .newMessage()
                    .value((body))
                    .property(ROP_MESSAGE_ID, msgId)
                    .sendAsync();
            CompletableFuture<org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult> offsetFuture = messageIdFuture.thenApply((Function<MessageId, org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult>) messageId -> {
                try {
                    long cost = System.currentTimeMillis() - startTimeMs;
                    if (cost >= 1000) {
                        log.warn("RoP sendMessage timeout. Cost = [{}ms], topic: [{}]", cost, pTopic);
                    }
                    if (messageId == null) {
                        log.warn("Rop send delay level message error, messageId is null.");
                        return null;
                    }
                    return new org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult(msgId, messageId.toString(),
                            MessageIdUtils.getOffset((MessageIdImpl) messageId), msgKey, msgTag);
                } catch (Exception e) {
                    log.warn("Rop send delay level message error.", e);
                    return null;
                }
            });

            CompletableFuture<RopPutMessageResult> resultFuture = new CompletableFuture<>();

            offsetFuture.whenComplete((putMessageResult, t) -> {
                if (t != null || putMessageResult == null) {
                    log.warn("[{}] PutMessage error.", rmqTopic.getPulsarFullName(), t);
                    // remove pulsar topic from cache if send error
                    this.brokerController.getConsumerOffsetManager()
                            .removePulsarTopic(new ClientTopicName(messageInner.getTopic()), partitionId);

                    PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(status, temp);
                    resultFuture.complete(ropPutMessageResult);
                    return;
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(1);
                appendMessageResult.setWroteBytes(body.length);
                CommitLogOffset commitLogOffset = new CommitLogOffset(false, partitionId, putMessageResult.getOffset());
                String offsetMsgId = CommonUtils.createMessageId(storeHost, brokerController.getBrokerPort(),
                        commitLogOffset.getCommitLogOffset());
//                String offsetMsgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
//                        commitLogOffset.getCommitLogOffset());
                appendMessageResult.setMsgId(offsetMsgId);
                appendMessageResult.setLogicsOffset(putMessageResult.getOffset());
                appendMessageResult.setWroteOffset(commitLogOffset.getCommitLogOffset());
                RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);

                putMessageResult.setOffsetMsgId(offsetMsgId);
                putMessageResult.setPartition(partitionId);
                ropPutMessageResult.addPutMessageId(putMessageResult);
                resultFuture.complete(ropPutMessageResult);

                String[] pulsarMessageIdParams = putMessageResult.getPulsarMsgId().split(":");
                MessageId pulsarMessageId = new MessageIdImpl(Long.parseLong(pulsarMessageIdParams[0]), Long.parseLong(pulsarMessageIdParams[1]), partitionId);

                if (opMessage) {
                    TxnMessage txnMessage = txnMessages.get(messageInner.getPreparedTransactionOffset());
                    if (txnMessage != null) {
                        txnMessage.setCommitOrRollback(true);
                        txnMessages.remove(messageInner.getPreparedTransactionOffset());
                        opMessageConsumer.acknowledgeAsync(pulsarMessageId);
                        messageConsumer.acknowledgeAsync(txnMessage.getMessageId());
                    } else {
                        log.warn("Can not get txn message via op message, offset {}", messageInner.getQueueOffset());
                    }
                } else {
                    TxnMessage txnMessage = new TxnMessage();
                    txnMessage.setMessageId(pulsarMessageId);
                    txnMessage.setDataBytes(body);
                    txnMessage.setOffset(putMessageResult.getOffset());
                    txnMessages.put(putMessageResult.getOffset(), txnMessage);
                }
            });
            return resultFuture.get();
        } catch (Exception e) {
            log.error("", e);
        }
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
    }

    public PutMessageResult putRealMessage(MessageExtBrokerInner messageInner) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("method {}, data {}", "putRealMessage", messageInner);
            }
            long startTimeMs = System.currentTimeMillis();
            final String msgId = messageInner.getProperties().get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            final String msgKey = messageInner.getProperties().get(PROPERTY_KEYS);
            final String msgTag = messageInner.getProperties().get(PROPERTY_TAGS);
            byte[] body = this.entryFormatter.encode(messageInner);
            int partitionId = messageInner.getQueueId();

            RocketMQTopic rmqTopic = new RocketMQTopic(messageInner.getTopic());
            final String pTopic = rmqTopic.getPartitionName(partitionId);

            if (log.isDebugEnabled()) {
                log.debug("method {}, pulsar topic {}", "putRealMessage", pTopic);
            }

            CompletableFuture<MessageId> messageIdFuture = getProducerFromCache(pTopic)
                    .newMessage()
                    .value(body)
                    .property(ROP_MESSAGE_ID, msgId)
                    .sendAsync();
            CompletableFuture<org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult> offsetFuture = messageIdFuture.thenApply((Function<MessageId, org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult>) messageId -> {
                try {
                    long cost = System.currentTimeMillis() - startTimeMs;
                    if (cost >= 1000) {
                        log.warn("RoP sendMessage timeout. Cost = [{}ms], topic: [{}]", cost, pTopic);
                    }
                    if (messageId == null) {
                        log.warn("Rop send delay level message error, messageId is null.");
                        return null;
                    }
                    return new org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult(msgId, messageId.toString(),
                            MessageIdUtils.getOffset((MessageIdImpl) messageId), msgKey, msgTag);
                } catch (Exception e) {
                    log.warn("Rop send delay level message error.", e);
                    return null;
                }
            });

            CompletableFuture<RopPutMessageResult> result = new CompletableFuture<>();

            offsetFuture.whenComplete((putMessageResult, t) -> {
                if (t != null || putMessageResult == null) {
                    log.warn("[{}] PutMessage error.", rmqTopic.getPulsarFullName(), t);
                    // remove pulsar topic from cache if send error
                    this.brokerController.getConsumerOffsetManager()
                            .removePulsarTopic(new ClientTopicName(messageInner.getTopic()), partitionId);

                    PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(status, temp);
                    result.complete(ropPutMessageResult);
                    return;
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(1);
                appendMessageResult.setWroteBytes(body.length);
                CommitLogOffset commitLogOffset = new CommitLogOffset(false, partitionId, putMessageResult.getOffset());
                String offsetMsgId = CommonUtils.createMessageId(storeHost, brokerController.getBrokerPort(),
                        commitLogOffset.getCommitLogOffset());
//                String offsetMsgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
//                        commitLogOffset.getCommitLogOffset());
                appendMessageResult.setMsgId(offsetMsgId);
                appendMessageResult.setLogicsOffset(putMessageResult.getOffset());
                appendMessageResult.setWroteOffset(commitLogOffset.getCommitLogOffset());
                RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);

                putMessageResult.setOffsetMsgId(offsetMsgId);
                putMessageResult.setPartition(partitionId);
                ropPutMessageResult.addPutMessageId(putMessageResult);
                result.complete(ropPutMessageResult);
            });
            return result.get();
        } catch (Exception e) {
            log.error("", e);
        }
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        try {
//            MessageIdImpl id = MessageIdUtils.getMessageId(commitLogOffset);
//            PulsarAdmin pulsarAdmin = brokerController.getBrokerService().getPulsar().getAdminClient();
//            RocketMQTopic rmqTopic = new RocketMQTopic(TransactionalMessageUtil.buildHalfTopic());
//            final String pTopic = rmqTopic.getPartitionName(0);
//
//            Message<byte[]> message = pulsarAdmin.topics().getMessageById(pTopic, id.getLedgerId(), id.getEntryId());
//            MessageExt messageExt = RopEntryFormatter.decodePulsarMessage(message);

            TxnMessage txnMessage = txnMessages.get(commitLogOffset);
            if (txnMessage == null) {
                log.warn("Can not get txn message of {}", commitLogOffset);
                return null;
            }
            MessageExt messageExt = RopEntryFormatter.decodePulsarMessage(txnMessage.getDataBytes(), txnMessage.getMessageId());
            messageExt.setQueueOffset(txnMessage.getOffset());
            messageExt.setCommitLogOffset(txnMessage.getOffset());
            messageExt.setPreparedTransactionOffset(txnMessage.getOffset());
            if (log.isDebugEnabled()) {
                log.debug("method {}, pulsarMessageId {}, rocketmqMessage {}",
                        "lookMessageByOffset", txnMessage.getMessageId(), messageExt);
            }
            return messageExt;
        } catch (Exception e) {
            log.error("Can not parse txn message of {}", commitLogOffset, e);
        }
        return null;
    }

    public Collection<TxnMessage> getTxnMessages() {
        return txnMessages.values();
    }

    public int currentTxnMessageCount() {
        return txnMessages.size();
    }

    private Producer<byte[]> getProducerFromCache(String pulsarTopic) {
        try {
            String pulsarProducerName = "rop-inner-txn-" + pulsarTopic;
            Producer<byte[]> producer = producers
                    .get(pulsarProducerName, () -> createNewProducer(pulsarTopic, pulsarProducerName));
            return producer;
        } catch (Exception e) {
            log.warn("getProducerFromCache[topic={}] error.", pulsarTopic, e);
        }
        return null;
    }

    private Producer<byte[]> createNewProducer(String pTopic, String producerGroup)
            throws PulsarClientException {
        ProducerBuilder<byte[]> producerBuilder = brokerController.getRopBrokerProxy().getPulsarClient()
                .newProducer()
                .maxPendingMessages(100);

        return producerBuilder.clone()
                .topic(pTopic)
                .producerName(producerGroup + CommonUtils.UNDERSCORE_CHAR + System.currentTimeMillis())
                .sendTimeout(10, TimeUnit.SECONDS)
                .enableBatching(false)
                .blockIfQueueFull(false)
                .create();
    }

    private Consumer<byte[]> createPulsarConsumer(String topic) throws Exception {
        RocketMQTopic rmqTopic = new RocketMQTopic(topic);
        String pTopic = rmqTopic.getPartitionName(0);
        String pulsarSubscribeName = brokerController.getBrokerAddress();

        return brokerController.getBrokerService()
                .getPulsar()
                .getClient()
                .newConsumer()
                .messageListener(new PulsarTxnMessageListener(this))
                .topic(pTopic)
                .subscriptionName(pulsarSubscribeName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
    }
}
