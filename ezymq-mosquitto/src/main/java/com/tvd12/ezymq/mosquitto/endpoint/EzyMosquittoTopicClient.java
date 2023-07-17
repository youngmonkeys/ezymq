package com.tvd12.ezymq.mosquitto.endpoint;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMqttMessage;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public class EzyMosquittoTopicClient extends EzyMosquittoEndpoint {

    public EzyMosquittoTopicClient(
        MqttClient mqttClient,
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
            toMqttMessage(rpcTopic, props, message)
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
