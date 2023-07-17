package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public interface EzyMqttCallback{

    default void connectionLost(
        EzyMqttConnectionLostException exception
    ) {}

    void messageArrived(
        EzyMosquittoProperties properties,
        byte[] body
    ) throws Exception;
}