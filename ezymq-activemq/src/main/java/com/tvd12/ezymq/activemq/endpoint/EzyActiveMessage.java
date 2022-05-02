package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import lombok.Getter;

@Getter
public class EzyActiveMessage {

    protected final byte[] body;
    protected final EzyActiveProperties properties;

    public EzyActiveMessage(EzyActiveProperties properties, byte[] body) {
        this.body = body;
        this.properties = properties;
    }
}
