package com.tvd12.ezymq.common.handler;

public interface EzyMQMessageConsumer<T> {

    void consume(T message);
}
