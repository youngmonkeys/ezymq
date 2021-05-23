package com.tvd12.ezymq.kafka.handler;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyKafkaMessageHandlers {
	protected final Map<String, EzyKafkaMessageHandler> handlers = new HashMap<>();

    public void addHandler(String cmd, EzyKafkaMessageHandler handler) {
    		handlers.put(cmd, handler);
    }

    public EzyKafkaMessageHandler getHandler(String cmd) {
    	return handlers.get(cmd);
    }

	public Object handle(String cmd, Object message) throws Exception {
        EzyKafkaMessageHandler handler = getHandler(cmd);
        if(handler != null)
        	return handler.handle(message);
        return null;
    }
}
