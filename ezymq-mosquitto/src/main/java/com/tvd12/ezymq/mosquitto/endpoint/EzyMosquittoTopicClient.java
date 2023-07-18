package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMqttMqMessage;

public class EzyMosquittoTopicClient extends EzyMosquittoEndpoint {

    public EzyMosquittoTopicClient(
        EzyMqttClientProxy mqttClient,
        String topic
    ) {
        super(mqttClient, topic);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void publish(
        EzyMosquittoProperties props,
        byte[] message
    ) throws Exception {
        mqttClient.publish(
            topic,
            toMqttMqMessage(props, message)
        );
    }

    public static class Builder extends EzyMosquittoEndpoint.Builder<Builder> {

        @Override
        public EzyMosquittoTopicClient build() {
            return new EzyMosquittoTopicClient(
                mqttClient,
                topic
            );
        }
    }
}
