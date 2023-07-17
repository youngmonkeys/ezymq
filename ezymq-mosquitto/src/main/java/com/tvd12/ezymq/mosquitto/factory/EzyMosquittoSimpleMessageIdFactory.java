package com.tvd12.ezymq.mosquitto.factory;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EzyMosquittoSimpleMessageIdFactory
    extends EzyLoggable
    implements EzyMosquittoMessageIdFactory {

    private final Map<String, AtomicInteger> generatorByTopic =
        new ConcurrentHashMap<>();

    @Override
    public int newMessageId(String topic) {
        return generatorByTopic.compute(
            topic,
            (k, v) -> {
                AtomicInteger generator = v != null
                    ? v
                    : new AtomicInteger();
                if (generator.intValue() == Integer.MAX_VALUE) {
                    generator.set(0);
                }
                return generator;
            }
        ).incrementAndGet();
    }
}
