package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyRabbitTopicManager extends EzyRabbitAbstractManager {

    protected final EzyRabbitDataCodec dataCodec;
    protected final Map<String, EzyRabbitTopic> topics;
    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitTopicSetting> topicSettings;

    public EzyRabbitTopicManager(
        EzyRabbitDataCodec dataCodec,
        ConnectionFactory connectionFactory,
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitTopicSetting> topicSettings
    ) {
        super(connectionFactory);
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
            return createTopic(setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create topic: " + name, e);
        }
    }

    protected EzyRabbitTopic createTopic(
        EzyRabbitTopicSetting setting
    ) throws Exception {
        EzyRabbitTopicClient client = null;
        EzyRabbitTopicServer server = null;
        Channel channel = getChannel(setting);
        declareComponents(channel, setting);
        if (setting.isClientEnable()) {
            client = EzyRabbitTopicClient.builder()
                .channel(channel)
                .exchange(setting.getExchange())
                .routingKey(setting.getClientRoutingKey())
                .build();
        }
        if (setting.isServerEnable()) {
            server = EzyRabbitTopicServer.builder()
                .channel(channel)
                .exchange(setting.getExchange())
                .queueName(setting.getServerQueueName())
                .build();
        }
        return EzyRabbitTopic.builder()
            .dataCodec(dataCodec)
            .client(client)
            .server(server).build();
    }

    protected void declareComponents(
        Channel channel,
        EzyRabbitTopicSetting setting
    ) throws Exception {
        channel.basicQos(setting.getPrefetchCount());
        channel.exchangeDeclare(
            setting.getExchange(),
            EzyRabbitExchangeTypes.FANOUT
        );
        if (setting.getServerQueueName() == null) {
            return;
        }
        Map<String, Object> requestQueueArguments =
            queueArguments.get(setting.getServerQueueName());
        channel.queueDeclare(
            setting.getServerQueueName(),
            false,
            false,
            false,
            requestQueueArguments
        );
        channel.queueBind(
            setting.getServerQueueName(),
            setting.getExchange(),
            setting.getClientRoutingKey()
        );
    }
}
