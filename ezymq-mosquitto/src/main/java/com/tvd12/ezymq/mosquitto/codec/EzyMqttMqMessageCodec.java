package com.tvd12.ezymq.mosquitto.codec;

import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface EzyMqttMqMessageCodec {

    MqttMessage encode(EzyMqttMqMessage mqttMqMessage);


    EzyMqttMqMessage decode(MqttMessage mqttMessage);
}