package org.streamnative.pulsar.handlers.rocketmq.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;

@Slf4j
public class PulsarTxnMessageListener implements MessageListener<byte[]> {

    private final TxnMessageStore store;

    public PulsarTxnMessageListener(TxnMessageStore store) {
        this.store = store;
    }

    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        try {
            long offset = MessageIdUtils.getOffset((MessageIdImpl) msg.getMessageId());
            MessageExt messageExt = RopEntryFormatter.decodePulsarMessage(msg);
            log.info("receive pulsar message {}", new String(msg.getData()));
//            store.onMessageReceived();
//            store.onOpMessageReceived();
            consumer.acknowledge(msg);
            // TODO
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
