package com.tencent.tdmq.handlers.rocketmq.utils;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:56 下午
 */

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 这个工具类主要用来将 RocketMQ 和 pulsar 中的 topic 组合关系进行映射
 */
public class TopicNameUtils {

    public static TopicName pulsarTopicName(MessageQueue topicPartition, NamespaceName namespace) {
        return pulsarTopicName(topicPartition.getTopic(), topicPartition.getQueueId(), namespace);
    }

    public static TopicName pulsarTopicName(MessageQueue topicPartition) {
        return pulsarTopicName(topicPartition.getTopic(), topicPartition.getQueueId());
    }

    private static TopicName pulsarTopicName(String topic, int partitionIndex) {
        return TopicName.get(topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
    }

    public static TopicName pulsarTopicName(String topic, NamespaceName namespace) {
        return TopicName.get(TopicDomain.persistent.value(), namespace, topic);
    }

    public static TopicName pulsarTopicName(String topic) {
        return TopicName.get(topic);
    }

    public static TopicName pulsarTopicName(String topic, int partitionIndex, NamespaceName namespace) {
        if (topic.startsWith(TopicDomain.persistent.value())) {
            topic = topic.replace(TopicDomain.persistent.value() + "://", "");
        }

        if (topic.contains(namespace.getNamespaceObject().toString())) {
            topic = topic.replace(namespace.getNamespaceObject().toString() + "/", "");
        }
        return TopicName.get(TopicDomain.persistent.value(),
                namespace,
                topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
    }

    public static String getPartitionedTopicNameWithoutPartitions(TopicName topicName) {
        String localName = topicName.getPartitionedTopicName();
        if (localName.contains(PARTITIONED_TOPIC_SUFFIX)) {
            return localName.substring(0, localName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX));
        } else {
            return localName;
        }
    }

    // get local name without partition part
    public static String getRocketmqTopicNameFromPulsarTopicName(TopicName topicName) {
        // remove partition part
        String localName = topicName.getPartitionedTopicName();
        // remove persistent://tenant/ns
        return TopicName.get(localName).getLocalName();
    }

}
