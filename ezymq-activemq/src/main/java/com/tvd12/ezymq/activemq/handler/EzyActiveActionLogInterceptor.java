package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyActiveActionLogInterceptor
    extends EzyLoggable
    implements EzyActiveActionInterceptor {

    @Override
    public void intercept(String cmd, Object requestData) {
        logger.info("request command: {} request data: {}", cmd, requestData);
    }

    @Override
    public void intercept(String cmd, Object requestData, Object responseData) {
        logger.info(
            "response command: {} request data: {}, response data: {}",
            cmd,
            requestData,
            responseData
        );
    }

    @Override
    public void intercept(String cmd, Object requestData, Exception e) {
        logger.info(
            "exception command: {} request data: {} exception: ",
            cmd,
            requestData,
            e
        );
    }
}
