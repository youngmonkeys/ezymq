package com.tvd12.ezymq.rabbitmq.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyRabbitRequestLogInterceptor
    extends EzyLoggable
    implements EzyRabbitRequestInterceptor {

    @Override
    public void preHandle(String cmd, Object request) {
        logger.info(
            "request command: {} request data: {}",
            cmd,
            request
        );
    }

    @Override
    public void postHandle(String cmd, Object request, Object response) {
        logger.info(
            "response command: {} request data: {}, response data: {}",
            cmd,
            request,
            response
        );
    }

    @Override
    public void postHandle(String cmd, Object requestData, Throwable e) {
        logger.info(
            "exception command: {} request data: {} exception: ",
            cmd,
            requestData,
            e
        );
    }
}
