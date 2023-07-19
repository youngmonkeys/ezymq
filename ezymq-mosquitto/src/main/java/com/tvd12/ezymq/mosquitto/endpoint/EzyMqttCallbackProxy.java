package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import lombok.AllArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
public class EzyMqttCallbackProxy
    extends EzyLoggable
    implements MqttCallback {

    private final EzyMqttMqMessageCodec mqttMqMessageCodec;
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
    ) {
        try {
            EzyMqttCallback callback = callbackByTopic.get(topic);
            if (callback == null) {
                return;
            }
            emitMessage(callback, mqttMessage);
        } catch (Exception e) {
            logger.info("topic: {} process arrived message error", topic,  e);
        }
    }

    private void emitMessage(
        EzyMqttCallback callback,
        MqttMessage mqttMessage
    ) throws Exception {
        EzyMqttMqMessage mqttMqMessage =
            mqttMqMessageCodec.decode(mqttMessage);
        EzyMosquittoProperties properties = EzyMosquittoProperties
            .builder()
            .messageId(mqttMessage.getId())
            .messageType(mqttMqMessage.getType())
            .correlationId(mqttMqMessage.getCorrelationId())
            .headers(mqttMqMessage.getHeaders())
            .qos(mqttMessage.getQos())
            .retained(mqttMessage.isRetained())
            .build();
        callback.messageArrived(
            properties,
            mqttMqMessage.getBody()
        );
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken mqttDeliveryToken) {
        // do nothing
    }
}
