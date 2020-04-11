package com.tvd12.ezymq.rabbitmq.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tvd12.ezyfox.util.EzyLoggable;

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

    public List<EzyRabbitMessageConsumer> getComsumers(String cmd) {
    	List<EzyRabbitMessageConsumer> answer = consumers.get(cmd);
        if (answer != null)
            return answer;
        return Collections.EMPTY_LIST;
    }

	public void consume(String cmd, Object message) {
		List<EzyRabbitMessageConsumer> consumerList = getComsumers(cmd);
		for(EzyRabbitMessageConsumer consumer : consumerList) {
			try {
				consumer.consume(message);
			}
			catch (Exception e) {
				logger.warn("consume command: {} message: {} error", cmd, message);
			}
		}
    }
}
