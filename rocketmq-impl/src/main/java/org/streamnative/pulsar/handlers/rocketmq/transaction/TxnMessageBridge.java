package org.streamnative.pulsar.handlers.rocketmq.transaction;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;

@Slf4j
public class TxnMessageBridge {

    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    private final RocketMQBrokerController brokerController;
    private final TxnMessageStore store;
    private final SocketAddress storeHost;

    public TxnMessageBridge(RocketMQBrokerController brokerController, TxnMessageStore messageStore) {
        try {
            this.brokerController = brokerController;
            this.store = messageStore;
            String brokerHost = RemotingUtil.getLocalAddress();;
            this.storeHost = new InetSocketAddress(brokerHost, brokerController.getBrokerPort());
        } catch (Exception e) {
            log.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }
    }

    // 半消息
    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner), false);
    }

    // 操作消息
    public boolean putOpMessage(MessageExt messageExt, String opType) {
        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
                this.brokerController.getBrokerHost(), messageExt.getQueueId());
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    public boolean putMessage(MessageExtBrokerInner messageInner, boolean opMessage) {
        PutMessageResult putMessageResult = store.putMessage(messageInner, opMessage);
        if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            log.error("Put message failed, topic: {}, queueId: {}, msgId: {}",
                    messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return store.lookMessageByOffset(commitLogOffset);
    }

    public Collection<TxnMessage> getTxnMessages() {
        return store.getTxnMessages();
    }

    public int currentTxnMessageCount() {
        return store.currentTxnMessageCount();
    }

    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
                String.valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));
        writeOp(message, messageQueue, messageExt.getPreparedTransactionOffset());
        return true;
    }

    private void writeOp(Message message, MessageQueue mq, long transactionOffset) {
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }
        putMessage(makeOpMessageInner(message, opQueue, transactionOffset), true);
    }

    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        opQueue.setBrokerName(halfMQ.getBrokerName());
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }

    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue, long transactionOffset) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(message.getTopic());
        msgInner.setBody(message.getBody());
        msgInner.setQueueId(messageQueue.getQueueId());
        msgInner.setTags(message.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, message.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setPreparedTransactionOffset(transactionOffset);
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                String.valueOf(msgInner.getQueueId()));
        msgInner.setSysFlag(
                MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }
}
