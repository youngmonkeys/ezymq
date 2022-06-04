package com.tvd12.ezymq.common.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezyfox.reflect.EzyGenerics;

public interface EzyMQMessageConsumer<T> {

    void consume(T message);

    default Class<?> getMessageType() {
        try {
            Class<?> consumerClass = getClass();
            Class<?>[] args = EzyGenerics.getGenericInterfacesArguments(
                consumerClass,
                EzyMQMessageConsumer.class,
                1
            );
            return args[0];
        } catch (Exception e) {
            throw new EzyNotImplementedException(
                "unknown message type of: " + getClass().getName() +
                    ", you must implement getMessageType method"
            );
        }
    }
}
