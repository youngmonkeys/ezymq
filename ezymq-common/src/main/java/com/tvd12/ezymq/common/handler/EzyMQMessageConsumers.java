package com.tvd12.ezymq.common.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyMQMessageConsumers<C extends EzyMQMessageConsumer>
    extends EzyLoggable {

    protected final Map<String, List<C>> consumers;

    public EzyMQMessageConsumers() {
        this.consumers = new HashMap<>();
    }

    public void addConsumer(String cmd, C consumer) {
        synchronized (consumers) {
            List<C> consumerList
                = consumers.computeIfAbsent(cmd, k -> new ArrayList<>());
            consumerList.add(consumer);
        }
    }

    public List<C> getConsumers(String cmd) {
        List<C> answer;
        synchronized (consumers) {
            answer = consumers.get(cmd);
        }
        if (answer != null) {
            return answer;
        }
        return Collections.EMPTY_LIST;
    }

    public void consume(String cmd, Object message) {
        List<C> consumerList = getConsumers(cmd);
        for (C consumer : consumerList) {
            try {
                consumer.consume(message);
            } catch (Exception e) {
                logger.warn("consume command: {} message: {} error", cmd, message);
            }
        }
    }
}
