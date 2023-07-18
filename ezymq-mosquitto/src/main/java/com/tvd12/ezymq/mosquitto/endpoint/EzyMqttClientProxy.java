package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import lombok.AllArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttClient;

@AllArgsConstructor
public class EzyMqttClientProxy {

    private final MqttClient mqttClient;
    private final EzyMqttMqMessageCodec mqttMqMessageCodec;
    private final EzyMqttCallbackProxy mqttCallbackProxy;

    public EzyMqttClientProxy(
        MqttClient mqttClient,
        EzyMqttMqMessageCodec mqttMqMessageCodec
    ) {
        this.mqttClient = mqttClient;
        this.mqttMqMessageCodec = mqttMqMessageCodec;
        this.mqttCallbackProxy = new EzyMqttCallbackProxy(
            mqttMqMessageCodec
        );
        this.mqttClient.setCallback(mqttCallbackProxy);
    }

    public void connect() throws Exception {
        mqttClient.connect();
    }

    public void registerCallback(String topic, EzyMqttCallback callback) {
        mqttCallbackProxy.registerCallback(topic, callback);
    }

    public void publish(
        String topic,
        EzyMqttMqMessage message
    ) throws Exception {
        mqttClient.publish(
            topic,
            mqttMqMessageCodec.encode(message)
        );
    }

    public void subscribe(String topicFilter) throws Exception {
        mqttClient.subscribe(topicFilter);
    }

    public void close() throws Exception {
        mqttClient.close();
    }
}
