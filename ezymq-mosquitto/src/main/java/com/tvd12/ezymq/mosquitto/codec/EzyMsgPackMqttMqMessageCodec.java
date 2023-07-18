package com.tvd12.ezymq.mosquitto.codec;

import com.tvd12.ezyfox.codec.EzyObjectDeserializer;
import com.tvd12.ezyfox.codec.EzyObjectSerializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleDeserializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleSerializer;
import com.tvd12.ezyfox.entity.EzyObject;
import com.tvd12.ezyfox.factory.EzyEntityFactory;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class EzyMsgPackMqttMqMessageCodec
    implements EzyMqttMqMessageCodec {

    private final EzyObjectSerializer objectSerializer;
    private final EzyObjectDeserializer objectDeserializer;

    public EzyMsgPackMqttMqMessageCodec() {
        this.objectSerializer = new MsgPackSimpleSerializer();
        this.objectDeserializer = new MsgPackSimpleDeserializer();
    }

    @Override
    public MqttMessage encode(EzyMqttMqMessage mqttMqMessage) {
        EzyObject payloadMap = EzyEntityFactory.newObject();
        payloadMap.put("type", mqttMqMessage.getType());
        payloadMap.put("headers", mqttMqMessage.getHeaders());
        payloadMap.put("body", mqttMqMessage.getBody());
        byte[] payload = objectSerializer.serialize(payloadMap);
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setId(mqttMqMessage.getId());
        mqttMessage.setQos(mqttMqMessage.getQos());
        mqttMessage.setRetained(mqttMqMessage.isRetained());
        mqttMessage.setPayload(payload);
        return mqttMessage;
    }

    @SuppressWarnings("unchecked")
    @Override
    public EzyMqttMqMessage decode(MqttMessage mqttMessage) {
        byte[] payload = mqttMessage.getPayload();
        EzyObject payloadMap = objectDeserializer.deserialize(payload);
        EzyObject headers = payloadMap.get("headers", EzyObject.class);
        return EzyMqttMqMessage.builder()
            .id(mqttMessage.getId())
            .qos(mqttMessage.getQos())
            .retained(mqttMessage.isRetained())
            .type(payloadMap.getWithDefault("type", ""))
            .headers(headers == null ? null : headers.toMap())
            .body(payloadMap.get("body", byte[].class))
            .build();
    }
}
