package com.tvd12.ezymq.kafka.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyKafkaMessageHandlers extends EzyLoggable {

    protected final Map<String, EzyKafkaMessageHandler> handlers =
        new HashMap<>();

    public void addHandler(String cmd, EzyKafkaMessageHandler handler) {
        this.handlers.put(cmd, handler);
    }

    public void addHandlers(Map<String, EzyKafkaMessageHandler> handlers) {
        this.handlers.putAll(handlers);
    }

    public EzyKafkaMessageHandler getHandler(String cmd) {
        return handlers.get(cmd);
    }

    public Object handle(String cmd, Object message) throws Exception {
        EzyKafkaMessageHandler handler = getHandler(cmd);
        if (handler != null) {
            return handler.handle(message);
        }
        throw new IllegalArgumentException("has no handler for command: " + cmd);
    }
}
