package com.tvd12.ezymq.mosquitto.endpoint;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMessage;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoMessageHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import lombok.Setter;

public class EzyMosquittoTopicServer
    extends EzyMosquittoEndpoint
    implements EzyStartable {

    @Setter
    protected EzyMosquittoMessageHandler messageHandler;
    protected final EzyMqttCallbackProxy mqttCallbackProxy;

    public EzyMosquittoTopicServer(
        MqttClient mqttClient,
        String topic,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        super(mqttClient, topic);
        this.mqttCallbackProxy = mqttCallbackProxy;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() throws Exception {
        mqttCallbackProxy.registerCallback(topic, new EzyMqttCallback() {
            @Override
            public void messageArrived(
                EzyMosquittoProperties properties,
                byte[] body
            ) {
                messageHandler.handle(
                    toMessage(rpcTopic, properties, body)
                );
            }
        });
    }

    public static class Builder extends EzyMosquittoEndpoint.Builder<Builder> {

        protected EzyMqttCallbackProxy mqttCallbackProxy;

        public Builder mqttCallbackProxy(EzyMqttCallbackProxy mqttCallbackProxy) {
            this.mqttCallbackProxy = mqttCallbackProxy;
            return this;
        }

        @Override
        public EzyMosquittoTopicServer build() {
            return new EzyMosquittoTopicServer(
                mqttClient,
                topic,
                mqttCallbackProxy
            );
        }
    }
}
