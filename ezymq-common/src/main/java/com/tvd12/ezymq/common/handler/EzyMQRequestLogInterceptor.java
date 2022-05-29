package com.tvd12.ezymq.common.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyMQRequestLogInterceptor
    extends EzyLoggable
    implements EzyMQRequestInterceptor {

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
