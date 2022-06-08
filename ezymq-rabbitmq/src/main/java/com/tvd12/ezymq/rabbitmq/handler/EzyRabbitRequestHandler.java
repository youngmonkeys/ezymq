package com.tvd12.ezymq.rabbitmq.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezyfox.reflect.EzyGenerics;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandler;

public interface EzyRabbitRequestHandler<R>
    extends EzyMQRequestHandler<R> {

    default Class<?> getRequestType() {
        try {
            Class<?> handlerClass = getClass();
            Class<?>[] args = EzyGenerics.getGenericInterfacesArguments(
                handlerClass,
                EzyRabbitRequestHandler.class,
                1
            );
            return args[0];
        } catch (Exception e) {
            throw new EzyNotImplementedException(
                "unknown request type of: " + getClass().getName() +
                    ", you must implement getRequestType method"
            );
        }
    }
}
