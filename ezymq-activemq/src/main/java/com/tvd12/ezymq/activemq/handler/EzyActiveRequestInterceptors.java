package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezymq.activemq.util.EzyActiveInterceptorComparator;
import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptors;

public class EzyActiveRequestInterceptors
    extends EzyMQRequestInterceptors<EzyActiveRequestInterceptor> {

    @Override
    protected void sortInterceptors() {
        interceptors.sort(EzyActiveInterceptorComparator.getInstance());
    }
}
