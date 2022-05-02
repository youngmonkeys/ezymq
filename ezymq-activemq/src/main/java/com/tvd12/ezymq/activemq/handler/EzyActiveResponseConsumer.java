package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public interface EzyActiveResponseConsumer {

    void consume(EzyActiveProperties properties, byte[] responseBody);
}
