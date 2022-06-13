package org.streamnative.pulsar.handlers.rocketmq.transaction;

import lombok.Data;
import org.apache.pulsar.client.api.MessageId;

@Data
public class TxnMessage {

    private byte[] dataBytes;
    // 事务主题中的消息ID
    private MessageId messageId;
    private boolean commitOrRollback;
    private long offset;
    private int queueId;

    private long addTime = System.currentTimeMillis();
    private int checkTimes;
    private long lastCheckTime = System.currentTimeMillis();
}
