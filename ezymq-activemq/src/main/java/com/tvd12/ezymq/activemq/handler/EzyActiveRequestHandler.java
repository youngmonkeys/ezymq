package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezyfox.reflect.EzyGenerics;

public interface EzyActiveRequestHandler<R> {

    default Object handle(R request) throws Exception {
        process(request);
        return Boolean.TRUE;
    }

    default void process(R request) throws Exception {}

    default Class<?> getRequestType() {
        try {
            Class<?> handlerClass = getClass();
            Class<?>[] args = EzyGenerics.getGenericInterfacesArguments(
                handlerClass,
                EzyActiveRequestHandler.class,
                2
            );
            return args[0];
        } catch (Exception e) {
            throw new IllegalStateException(
                "unknown request type of: " + getClass().getName() +
                    ", you must implement getRequestType method"
            );
        }
    }
}
