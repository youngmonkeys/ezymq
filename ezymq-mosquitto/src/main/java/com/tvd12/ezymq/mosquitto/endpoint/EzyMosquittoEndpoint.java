package com.tvd12.ezymq.mosquitto.endpoint;

import static com.tvd12.ezymq.mosquitto.constant.EzyMosquittoConstants.RPC_TOPIC_PREFIX;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyMosquittoEndpoint
    extends EzyLoggable
    implements EzyCloseable {

    protected final String topic;
    protected final boolean rpcTopic;
    protected final MqttClient mqttClient;

    public EzyMosquittoEndpoint(
        MqttClient mqttClient,
        String topic
    ) {
        this.topic = topic;
        this.mqttClient = mqttClient;
        this.rpcTopic = topic.startsWith(RPC_TOPIC_PREFIX);
    }

    @Override
    public void close() {}

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyMosquittoEndpoint> {

        protected String topic;
        protected MqttClient mqttClient;

        public B topic(String topic) {
            this.topic = topic;
            return (B) this;
        }

        public B mqttClient(MqttClient mqttClient) {
            this.mqttClient = mqttClient;
            return (B) this;
        }
    }
}
