package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveTopicSetting;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyActiveTopicManager
    extends EzyActiveAbstractManager implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyActiveTopic> topics;
    protected final Map<String, EzyActiveTopicSetting> topicSettings;

    public EzyActiveTopicManager(
        EzyMQDataCodec dataCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyActiveTopicSetting> topicSettings
    ) {
        super(connectionFactory);
        this.dataCodec = dataCodec;
        this.topicSettings = topicSettings;
        this.topics = createTopics();
    }

    public <T> EzyActiveTopic<T> getTopic(String name) {
        EzyActiveTopic<T> topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("has no topic with name: " + name);
        }
        return topic;
    }

    protected Map<String, EzyActiveTopic> createTopics() {
        Map<String, EzyActiveTopic> map = new HashMap<>();
        for (String name : topicSettings.keySet()) {
            EzyActiveTopicSetting setting = topicSettings.get(name);
            map.put(name, createTopic(name, setting));
        }
        return map;
    }

    protected EzyActiveTopic createTopic(
        String name,
        EzyActiveTopicSetting setting
    ) {
        try {
            return doCreateTopic(name, setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create topic: " + name, e);
        }
    }

    protected EzyActiveTopic doCreateTopic(
        String name,
        EzyActiveTopicSetting setting
    ) throws Exception {
        EzyActiveTopicClient client = null;
        EzyActiveTopicServer server = null;
        Session session = getSession(setting);
        if (setting.isProducerEnable()) {
            client = EzyActiveTopicClient.builder()
                .session(session)
                .topic(setting.getTopic())
                .topicName(setting.getTopicName())
                .build();
        }
        if (setting.isConsumerEnable()) {
            server = EzyActiveTopicServer.builder()
                .session(session)
                .topic(setting.getTopic())
                .topicName(setting.getTopicName())
                .build();
        }
        EzyActiveTopic topic = EzyActiveTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .server(server)
            .build();
        topic.addConsumers(setting.getMessageConsumersByTopic());
        return topic;
    }

    @Override
    public void close() {
        for (EzyActiveTopic topic : topics.values()) {
            topic.close();
        }
    }
}
