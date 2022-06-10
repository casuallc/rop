package org.streamnative.pulsar.handlers.rocketmq.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.AbstractTransactionalMessageCheckListener;

@Slf4j
public class TxnMessageCheckListener extends AbstractTransactionalMessageCheckListener {

    public TxnMessageCheckListener(RocketMQBrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error(
                "MsgExt:{} has been checked too many times, so discard it by moving it to system topic "
                        + "TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner, false);
            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info(
                        "Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. "
                                + "Restored in queueOffset={}, commitLogOffset={}, real topic={}",
                        msgExt.getQueueOffset(),
                        msgExt.getCommitLogOffset(),
                        msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            } else {
                log.error(
                        "Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, "
                                + "real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e.getMessage());
        }

    }

    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager()
                .createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);
        int queueId = 0;
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(topicConfig.getTopicName());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(queueId);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
