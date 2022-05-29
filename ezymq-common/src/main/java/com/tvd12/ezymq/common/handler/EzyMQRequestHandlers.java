package com.tvd12.ezymq.common.handler;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyMQRequestHandlers<H extends EzyMQRequestHandler> {

    protected final Map<String, H> handlers;

    public EzyMQRequestHandlers() {
        this.handlers = new HashMap<>();
    }

    public void addHandler(String cmd, H handler) {
        handlers.put(cmd, handler);
    }

    public void addHandlers(Map<String, H> handlers) {
        this.handlers.putAll(handlers);
    }

    public H getHandler(String cmd) {
        H handler = handlers.get(cmd);
        if (handler != null) {
            return handlers.get(cmd);
        }
        throw new IllegalArgumentException("has no handler with command: " + cmd);
    }

    public Object handle(String cmd, Object request) throws Exception {
        H handler = getHandler(cmd);
        return handler.handle(request);
    }
}
