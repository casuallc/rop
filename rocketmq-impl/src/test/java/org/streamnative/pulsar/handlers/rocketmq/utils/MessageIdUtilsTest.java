package org.streamnative.pulsar.handlers.rocketmq.utils;

import org.apache.pulsar.client.api.MessageId;
import org.junit.Test;

public class MessageIdUtilsTest {

    @Test
    public void testOffsetToMessageId() {
        long offset = 55012491265L;
        offset = 60934848513L;
        MessageId id = MessageIdUtils.getMessageId(offset);
        System.out.println(id);
    }
}
