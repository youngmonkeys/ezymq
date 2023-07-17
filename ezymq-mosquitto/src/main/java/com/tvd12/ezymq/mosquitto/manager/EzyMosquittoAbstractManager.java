package com.tvd12.ezymq.mosquitto.manager;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.util.EzyLoggable;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EzyMosquittoAbstractManager extends EzyLoggable {

    protected final MqttClient mqttClient;
}
