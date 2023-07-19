package com.tvd12.ezymq.mosquitto.manager;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoTopicSetting;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyMosquittoTopicManager
    extends EzyMosquittoAbstractManager
    implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyMosquittoTopic> topics;
    protected final Map<String, EzyMosquittoTopicSetting> topicSettings;

    public EzyMosquittoTopicManager(
        EzyMqttClientProxy mqttClient,
        EzyMQDataCodec dataCodec,
        Map<String, EzyMosquittoTopicSetting> topicSettings
    ) {
        super(mqttClient);
        this.dataCodec = dataCodec;
        this.topicSettings = topicSettings;
        this.topics = createTopics();
    }

    public <T> EzyMosquittoTopic<T> getTopic(String name) {
        EzyMosquittoTopic<T> topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("has no topic with name: " + name);
        }
        return topic;
    }

    protected Map<String, EzyMosquittoTopic> createTopics() {
        Map<String, EzyMosquittoTopic> map = new HashMap<>();
        for (String name : topicSettings.keySet()) {
            EzyMosquittoTopicSetting setting = topicSettings.get(name);
            map.put(name, createTopic(name, setting));
        }
        return map;
    }

    protected EzyMosquittoTopic createTopic(
        String name,
        EzyMosquittoTopicSetting setting
    ) {
        try {
            return doCreateTopic(name, setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create topic: " + name, e);
        }
    }

    protected EzyMosquittoTopic doCreateTopic(
        String name,
        EzyMosquittoTopicSetting setting
    ) throws Exception {
        EzyMosquittoTopicClient client = null;
        EzyMosquittoTopicServer server = null;
        if (setting.isProducerEnable()) {
            client = EzyMosquittoTopicClient
                .builder()
                .mqttClient(mqttClient)
                .topic(name)
                .build();
        }
        if (setting.isConsumerEnable()) {
            server = EzyMosquittoTopicServer
                .builder()
                .mqttClient(mqttClient)
                .topic(name)
                .build();
        }
        EzyMosquittoTopic topic = EzyMosquittoTopic
            .builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .server(server)
            .build();
        topic.addConsumers(setting.getMessageConsumersByTopic());
        mqttClient.subscribe(name);
        return topic;
    }

    @Override
    public void close() {
        for (EzyMosquittoTopic topic : topics.values()) {
            topic.close();
        }
    }
}
