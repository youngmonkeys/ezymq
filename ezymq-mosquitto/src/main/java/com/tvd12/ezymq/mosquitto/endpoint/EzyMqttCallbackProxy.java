package com.tvd12.ezymq.mosquitto.endpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public class EzyMqttCallbackProxy implements MqttCallback {

    private final Map<String, EzyMqttCallback> callbackByTopic =
        new ConcurrentHashMap<>();

    public void registerCallback(String topic, EzyMqttCallback callback) {
        callbackByTopic.put(topic, callback);
    }

    @Override
    public void connectionLost(Throwable throwable) {
        EzyMqttConnectionLostException e =
            new EzyMqttConnectionLostException(throwable);
        callbackByTopic.values().forEach(it -> it.connectionLost(e));
    }

    @Override
    public void messageArrived(
        String topic,
        MqttMessage mqttMessage
    ) throws Exception {
        EzyMqttCallback callback = callbackByTopic.get(topic);
        if (callback == null) {
            return;
        }
        EzyMosquittoProperties properties = EzyMosquittoProperties
            .builder()
            .messageId(mqttMessage.getId())
            .qos(mqttMessage.getQos())
            .retained(mqttMessage.isRetained())
            .build();
        callback.messageArrived(
            properties,
            mqttMessage.getPayload()
        );
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        // do nothing
    }
}
