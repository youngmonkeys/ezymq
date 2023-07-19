package com.tvd12.ezymq.mosquitto.factory;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.concurrent.atomic.AtomicLong;

public class EzyMosquittoSimpleCorrelationIdFactory
    extends EzyLoggable
    implements EzyMosquittoCorrelationIdFactory {

    private final AtomicLong generator = new AtomicLong();

    @Override
    public String newCorrelationId(String topic) {
        return topic + generator.incrementAndGet();
    }
}
