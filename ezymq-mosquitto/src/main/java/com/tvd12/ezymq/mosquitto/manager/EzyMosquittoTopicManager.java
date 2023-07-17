package com.tvd12.ezymq.mosquitto.manager;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoTopicSetting;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyMosquittoTopicManager
    extends EzyMosquittoAbstractManager
    implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyMosquittoTopic> topics;
    protected final Map<String, EzyMosquittoTopicSetting> topicSettings;

    public EzyMosquittoTopicManager(
        MqttClient mqttClient,
        EzyMqttCallbackProxy mqttCallbackProxy,
        EzyMQDataCodec dataCodec,
        Map<String, EzyMosquittoTopicSetting> topicSettings
    ) {
        super(mqttClient);
        this.dataCodec = dataCodec;
        this.topicSettings = topicSettings;
        this.topics = createTopics(mqttCallbackProxy);
    }

    public <T> EzyMosquittoTopic<T> getTopic(String name) {
        EzyMosquittoTopic<T> topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("has no topic with name: " + name);
        }
        return topic;
    }

    protected Map<String, EzyMosquittoTopic> createTopics(
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        Map<String, EzyMosquittoTopic> map = new HashMap<>();
        for (String name : topicSettings.keySet()) {
            EzyMosquittoTopicSetting setting = topicSettings.get(name);
            map.put(name, createTopic(name, setting, mqttCallbackProxy));
        }
        return map;
    }

    protected EzyMosquittoTopic createTopic(
        String name,
        EzyMosquittoTopicSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        try {
            return doCreateTopic(name, setting, mqttCallbackProxy);
        } catch (Exception e) {
            throw new IllegalStateException("can't create topic: " + name, e);
        }
    }

    protected EzyMosquittoTopic doCreateTopic(
        String name,
        EzyMosquittoTopicSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
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
                .mqttCallbackProxy(mqttCallbackProxy)
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
        return topic;
    }

    @Override
    public void close() {
        for (EzyMosquittoTopic topic : topics.values()) {
            topic.close();
        }
    }
}
