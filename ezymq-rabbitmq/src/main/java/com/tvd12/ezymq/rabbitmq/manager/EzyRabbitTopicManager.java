package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyRabbitTopicManager
    extends EzyRabbitAbstractManager
    implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyRabbitTopic> topics;
    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitTopicSetting> topicSettings;

    public EzyRabbitTopicManager(
        Connection connection,
        EzyMQDataCodec dataCodec,
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitTopicSetting> topicSettings
    ) {
        super(connection);
        this.dataCodec = dataCodec;
        this.topicSettings = topicSettings;
        this.queueArguments = queueArguments;
        this.topics = createTopics();
    }

    public <T> EzyRabbitTopic<T> getTopic(String name) {
        EzyRabbitTopic<T> topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("has no topic with name: " + name);
        }
        return topic;
    }

    protected Map<String, EzyRabbitTopic> createTopics() {
        Map<String, EzyRabbitTopic> map = new HashMap<>();
        for (String name : topicSettings.keySet()) {
            EzyRabbitTopicSetting setting = topicSettings.get(name);
            map.put(name, createTopic(name, setting));
        }
        return map;
    }

    protected EzyRabbitTopic createTopic(
        String name,
        EzyRabbitTopicSetting setting
    ) {
        try {
            return doCreateTopic(name, setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create topic: " + name, e);
        }
    }

    protected EzyRabbitTopic doCreateTopic(
        String name,
        EzyRabbitTopicSetting setting
    ) throws Exception {
        EzyRabbitTopicClient client = null;
        EzyRabbitTopicServer server = null;
        Channel channel = getChannel(setting);
        declareComponents(channel, setting);
        if (setting.isProducerEnable()) {
            client = EzyRabbitTopicClient.builder()
                .channel(channel)
                .exchange(setting.getExchange())
                .routingKey(setting.getProducerRoutingKey())
                .build();
        }
        if (setting.isConsumerEnable()) {
            server = EzyRabbitTopicServer.builder()
                .channel(channel)
                .exchange(setting.getExchange())
                .queueName(setting.getConsumerQueueName())
                .build();
        }
        EzyRabbitTopic topic = EzyRabbitTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .server(server)
            .build();
        topic.addConsumers(setting.getMessageConsumersByTopic());
        return topic;
    }

    protected void declareComponents(
        Channel channel,
        EzyRabbitTopicSetting setting
    ) throws Exception {
        channel.basicQos(setting.getPrefetchCount());
        channel.exchangeDeclare(
            setting.getExchange(),
            EzyRabbitExchangeTypes.TOPIC
        );
        if (setting.getConsumerQueueName() == null) {
            return;
        }
        Map<String, Object> requestQueueArguments =
            queueArguments.get(setting.getConsumerQueueName());
        channel.queueDeclare(
            setting.getConsumerQueueName(),
            false,
            false,
            false,
            requestQueueArguments
        );
        channel.queueBind(
            setting.getConsumerQueueName(),
            setting.getExchange(),
            setting.getProducerRoutingKey()
        );
    }

    @Override
    public void close() {
        for (EzyRabbitTopic topic : topics.values()) {
            topic.close();
        }
    }
}
