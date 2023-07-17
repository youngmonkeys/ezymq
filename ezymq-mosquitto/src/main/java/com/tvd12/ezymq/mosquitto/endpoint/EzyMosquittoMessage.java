package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import lombok.Getter;

@Getter
public class EzyMosquittoMessage {

    protected final byte[] body;
    protected final EzyMosquittoProperties properties;

    public EzyMosquittoMessage(EzyMosquittoProperties properties, byte[] body) {
        this.body = body;
        this.properties = properties;
    }
}
