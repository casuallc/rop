package org.streamnative.pulsar.handlers.rocketmq.transaction;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.TransactionalMessageService;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.AbstractTransactionalMessageCheckListener;

@Slf4j
public class TxnMessageServiceImpl implements TransactionalMessageService {
    private final TxnMessageBridge txnMessageBridge;
    private final RocketMQBrokerController brokerController;

    public TxnMessageServiceImpl(RocketMQBrokerController brokerController, TxnMessageBridge txnMessageBridge) {
        this.brokerController = brokerController;
        this.txnMessageBridge = txnMessageBridge;
        log.info("start");
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        log.info("method {}, data {}", "prepareMessage", messageInner);
        PutMessageResult result = txnMessageBridge.putHalfMessage(messageInner);
        log.info("method {}, result {}", "prepareMessage", result);
        return result;
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        log.info("method {}, data {}", "commitMessage", requestHeader);
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        log.info("method {}, data {}", "deletePrepareMessage", msgExt);
        if (this.txnMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        log.info("method {}, data {}", "rollbackMessage", requestHeader);
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
                      AbstractTransactionalMessageCheckListener listener) {
        int checkCount = 0, sendCount = 0;
        try {
            Collection<TxnMessage> txnMessages = txnMessageBridge.getTxnMessages();
            for (TxnMessage txnMessage : txnMessages) {
                checkCount ++;
                if (txnMessage.isCommitOrRollback()) {
                    continue;
                }

                if (System.currentTimeMillis() - txnMessage.getLastCheckTime() < 30000) {
                    continue;
                }

                MessageExt messageExt = txnMessageBridge.lookMessageByOffset(txnMessage.getOffset());
                listener.resolveHalfMsg(messageExt);
                txnMessage.setLastCheckTime(System.currentTimeMillis());
                sendCount ++;
            }
        } catch (Exception e) {
            log.error("" ,e);
        }
        log.info("Transaction check count {}, send count {}, current txn message count {}",
                checkCount, sendCount, txnMessageBridge.currentTxnMessageCount());
        log.info("method {}, data {} - {}", "check", transactionTimeout, transactionCheckMax);
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.txnMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean open() {
        log.info("method {}, data {}", "open", null);
        return true;
    }

    @Override
    public void close() {
        log.info("method {}, data {}", "close", null);
    }
}
