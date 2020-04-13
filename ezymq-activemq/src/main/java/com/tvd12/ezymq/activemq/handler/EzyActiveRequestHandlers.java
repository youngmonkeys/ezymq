package com.tvd12.ezymq.activemq.handler;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyActiveRequestHandlers {
	
	protected final Map<String, EzyActiveRequestHandler> handlers;
	
	public EzyActiveRequestHandlers() {
		this.handlers = new HashMap<>();
	}

    public void addHandler(String cmd, EzyActiveRequestHandler handler) {
    	handlers.put(cmd, handler);
    }

    public EzyActiveRequestHandler getHandler(String cmd) {
    	EzyActiveRequestHandler handler = handlers.get(cmd);
        if (handler != null)
            return handlers.get(cmd);
        throw new IllegalArgumentException("has no handler with command: " + cmd);
    }

	public Object handle(String cmd, Object request) throws Exception {
        EzyActiveRequestHandler handler = getHandler(cmd);
        Object answer = handler.handle(request);
        return answer;
    }
}
