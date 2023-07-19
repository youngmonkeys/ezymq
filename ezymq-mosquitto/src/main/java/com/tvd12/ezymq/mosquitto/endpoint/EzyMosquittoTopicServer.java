package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoMessageHandler;
import lombok.Setter;

public class EzyMosquittoTopicServer
    extends EzyMosquittoEndpoint
    implements EzyStartable {

    @Setter
    protected EzyMosquittoMessageHandler messageHandler;

    public EzyMosquittoTopicServer(
        EzyMqttClientProxy mqttClient,
        String topic
    ) {
        super(mqttClient, topic);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() throws Exception {
        mqttClient.registerCallback(topic, (properties, body) ->
            messageHandler.handle(properties, body)
        );
    }

    public static class Builder extends EzyMosquittoEndpoint.Builder<Builder> {

        @Override
        public EzyMosquittoTopicServer build() {
            return new EzyMosquittoTopicServer(
                mqttClient,
                topic
            );
        }
    }
}
