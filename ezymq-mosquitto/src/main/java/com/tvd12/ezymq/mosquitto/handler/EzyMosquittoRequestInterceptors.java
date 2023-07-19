package com.tvd12.ezymq.mosquitto.handler;

import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptors;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoInterceptorComparator;

public class EzyMosquittoRequestInterceptors
    extends EzyMQRequestInterceptors<EzyMosquittoRequestInterceptor> {

    @Override
    protected void sortInterceptors() {
        interceptors.sort(EzyMosquittoInterceptorComparator.getInstance());
    }
}
