package com.tvd12.ezymq.mosquitto.manager;

import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EzyMosquittoAbstractManager extends EzyLoggable {

    protected final EzyMqttClientProxy mqttClient;
}
