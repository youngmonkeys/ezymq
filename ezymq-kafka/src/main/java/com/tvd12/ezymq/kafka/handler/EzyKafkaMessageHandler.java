package com.tvd12.ezymq.kafka.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezyfox.reflect.EzyGenerics;

public interface EzyKafkaMessageHandler<T> {

    default Object handle(T message) throws Exception {
        process(message);
        return Boolean.TRUE;
    }

    default void process(T message) throws Exception {}

    default Class<?> getMessageType() {
        try {
            Class<?> handlerClass = getClass();
            Class<?>[] args = EzyGenerics.getGenericInterfacesArguments(
                handlerClass,
                EzyKafkaMessageHandler.class,
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
