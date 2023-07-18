package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyMosquittoEndpoint
    extends EzyLoggable
    implements EzyCloseable {

    protected final String topic;
    protected final EzyMqttClientProxy mqttClient;

    public EzyMosquittoEndpoint(
        EzyMqttClientProxy mqttClient,
        String topic
    ) {
        this.topic = topic;
        this.mqttClient = mqttClient;
    }

    @Override
    public void close() {}

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyMosquittoEndpoint> {

        protected String topic;
        protected EzyMqttClientProxy mqttClient;

        public B topic(String topic) {
            this.topic = topic;
            return (B) this;
        }

        public B mqttClient(EzyMqttClientProxy mqttClient) {
            this.mqttClient = mqttClient;
            return (B) this;
        }
    }
}
