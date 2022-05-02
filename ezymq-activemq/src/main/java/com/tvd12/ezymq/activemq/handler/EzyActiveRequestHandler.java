package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveRequestHandler<I, O> {

    O handle(I request) throws Exception;
}
