package com.tvd12.ezymq.rabbitmq.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyRabbitMessageConsumers extends EzyLoggable {

    protected final Map<String, List<EzyRabbitMessageConsumer>> consumers;

    public EzyRabbitMessageConsumers() {
        this.consumers = new HashMap<>();
    }

    public void addConsumer(String cmd, EzyRabbitMessageConsumer consumer) {
        synchronized (consumers) {
            List<EzyRabbitMessageConsumer> consumerList
                = consumers.computeIfAbsent(cmd, k -> new ArrayList<>());
            consumerList.add(consumer);
        }
    }

    public List<EzyRabbitMessageConsumer> getConsumers(String cmd) {
        List<EzyRabbitMessageConsumer> answer = null;
        synchronized (consumers) {
            answer = consumers.get(cmd);
        }
        if (answer != null) {
            return answer;
        }
        return Collections.EMPTY_LIST;
    }

    public void consume(String cmd, Object message) {
        List<EzyRabbitMessageConsumer> consumerList = getConsumers(cmd);
        for (EzyRabbitMessageConsumer consumer : consumerList) {
            try {
                consumer.consume(message);
            } catch (Exception e) {
                logger.warn("consume command: {} message: {} error", cmd, message);
            }
        }
    }
}
