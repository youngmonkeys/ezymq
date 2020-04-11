package com.tvd12.ezymq.rabbitmq.handler;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyRabbitRequestHandlers {
	
	protected final Map<String, EzyRabbitRequestHandler> handlers;
	
	public EzyRabbitRequestHandlers() {
		this.handlers = new HashMap<>();
	}

    public void addHandler(String cmd, EzyRabbitRequestHandler handler) {
    	handlers.put(cmd, handler);
    }

    public EzyRabbitRequestHandler getHandler(String cmd) {
    	EzyRabbitRequestHandler handler = handlers.get(cmd);
        if (handler != null)
            return handlers.get(cmd);
        throw new IllegalArgumentException("has no handler with command: " + cmd);
    }

	public Object handle(String cmd, Object request) throws Exception {
        EzyRabbitRequestHandler handler = getHandler(cmd);
        Object answer = handler.handle(request);
        return answer;
    }
}
