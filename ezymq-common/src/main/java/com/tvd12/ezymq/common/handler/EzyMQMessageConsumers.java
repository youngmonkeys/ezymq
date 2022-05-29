package com.tvd12.ezymq.common.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("rawtypes")
public class EzyMQMessageConsumers extends EzyLoggable {

    protected final Map<String, List<EzyMQMessageConsumer>> consumers =
        new ConcurrentHashMap<>();

    public void addConsumer(String cmd, EzyMQMessageConsumer consumer) {
        consumers
            .computeIfAbsent(
                cmd,
                k -> Collections.synchronizedList(new ArrayList<>())
            )
            .add(consumer);
    }

    @SuppressWarnings("unchecked")
    public void consume(String cmd, Object message) {
        List<EzyMQMessageConsumer> consumerList =
            consumers.getOrDefault(cmd, Collections.emptyList());
        for (EzyMQMessageConsumer consumer : consumerList) {
            try {
                consumer.consume(message);
            } catch (Exception e) {
                logger.warn(
                    "consume command: {} message: {} error",
                    cmd,
                    message
                );
            }
        }
    }
}
