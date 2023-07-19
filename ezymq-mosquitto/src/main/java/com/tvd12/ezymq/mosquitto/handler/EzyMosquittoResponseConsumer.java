package com.tvd12.ezymq.mosquitto.handler;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public interface EzyMosquittoResponseConsumer {

    void consume(EzyMosquittoProperties properties, byte[] responseBody);
}
