package com.tvd12.ezymq.activemq.factory;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.concurrent.atomic.AtomicLong;

public class EzyActiveSimpleCorrelationIdFactory
    extends EzyLoggable
    implements EzyActiveCorrelationIdFactory {

    private final String prefix;
    private final AtomicLong generator = new AtomicLong();

    public EzyActiveSimpleCorrelationIdFactory() {
        this("");
    }

    public EzyActiveSimpleCorrelationIdFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String newCorrelationId() {
        return prefix + generator.incrementAndGet();
    }
}
