package com.tvd12.ezymq.mosquitto.exception;

public class EzyMqttConnectionLostException extends IllegalStateException {

    public EzyMqttConnectionLostException(Throwable e) {
        super("Mqtt connection lost", e);
    }
}
