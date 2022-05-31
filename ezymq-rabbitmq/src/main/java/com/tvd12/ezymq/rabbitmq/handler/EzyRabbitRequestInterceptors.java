package com.tvd12.ezymq.rabbitmq.handler;

import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptors;
import com.tvd12.ezymq.rabbitmq.util.EzyRabbitInterceptorComparator;

public class EzyRabbitRequestInterceptors
    extends EzyMQRequestInterceptors<EzyRabbitRequestInterceptor> {

    @Override
    protected void sortInterceptors() {
        interceptors.sort(EzyRabbitInterceptorComparator.getInstance());
    }
}
