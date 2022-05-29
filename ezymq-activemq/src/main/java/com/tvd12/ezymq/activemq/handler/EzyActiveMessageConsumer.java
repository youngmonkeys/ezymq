package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;

public interface EzyActiveMessageConsumer<T>
    extends EzyMQMessageConsumer<T> {

    void consume(T message);
}
