package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.RpcClient.Response;
import com.rabbitmq.client.ShutdownSignalException;
import com.tvd12.ezyfox.concurrent.EzyFuture;
import com.tvd12.ezyfox.concurrent.EzyFutureConcurrentHashMap;
import com.tvd12.ezyfox.concurrent.EzyFutureMap;
import com.tvd12.ezymq.rabbitmq.exception.EzyRabbitMaxCapacity;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitSimpleCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;

public class EzyRabbitRpcClient extends EzyRabbitEndpoint {

	protected final int capacity;
    protected final int defaultTimeout;
	protected final String replyQueueName;
	protected final String replyRoutingKey;
	protected final String requestRoutingKey;
	protected final EzyFutureMap<String> futureMap;
	protected final EzyRabbitCorrelationIdFactory correlationIdFactory;
	protected final EzyRabbitResponseConsumer unconsumedResponseConsumer;
	private DefaultConsumer consumer;
	
	protected final static int NO_TIMEOUT = -1;
	
	public EzyRabbitRpcClient(
			Channel channel, 
			String exchange, 
			String routingKey,
			String replyQueueName,
			String replyRoutingKey, 
			int capacity,
			int defaultTimeout,
			EzyRabbitResponseConsumer unconsumedResponseConsumer) throws IOException {
        this(channel, 
        		exchange, 
        		routingKey, 
        		replyQueueName,
        		replyRoutingKey,
        		capacity,
        		defaultTimeout, 
        		new EzyRabbitSimpleCorrelationIdFactory(),
        		unconsumedResponseConsumer);
    }
	
	public EzyRabbitRpcClient(
			Channel channel, 
			String exchange, 
			String requestRoutingKey,
			String replyQueueName,
			String replyRoutingKey,
			int capacity,
			int defaultTimeout, 
			EzyRabbitCorrelationIdFactory correlationIdFactory,
			EzyRabbitResponseConsumer unconsumedResponseConsumer) throws IOException {
		super(channel, exchange);
		this.capacity = capacity;
        this.requestRoutingKey = requestRoutingKey;
        this.replyQueueName = replyQueueName;
        this.replyRoutingKey = replyRoutingKey;
        this.defaultTimeout = defaultTimeout;
        this.correlationIdFactory = correlationIdFactory;
        this.futureMap = new EzyFutureConcurrentHashMap<>(); 
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
        this.consumer = setupConsumer();
    }
	
	protected DefaultConsumer setupConsumer() throws IOException {
        DefaultConsumer newConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleShutdownSignal(String consumerTag,
                                             ShutdownSignalException signal) {
                Map<String, EzyFuture> remainFutures = futureMap.clear();
                for(EzyFuture future : remainFutures.values())
                	future.setResult(signal);
                consumer = null;
            }

            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
            	String replyId = properties.getCorrelationId();
                EzyFuture future = futureMap.removeFuture(replyId);
                if (future == null) {
            		if(unconsumedResponseConsumer != null) {
            			unconsumedResponseConsumer.consume(properties, body);
            		}
            		else { 
            			logger.warn("No outstanding request for correlation ID {}", replyId);
            		}
                } else {
                	future.setResult(new Response(consumerTag, envelope, properties, body));
                }
            }
        };
        channel.basicConsume(replyQueueName, true, newConsumer);
        return newConsumer;
    }
	
	public void doFire(AMQP.BasicProperties props, byte[] message)
	        throws IOException {
		checkConsumer();
        AMQP.BasicProperties.Builder propertiesBuilder = (props != null) 
        			? props.builder() 
        			: new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties newProperties = propertiesBuilder
        			.build();
        publish(newProperties, message);
	}
	
	public Response doCall(AMQP.BasicProperties props, byte[] message)
	        throws Exception {
		return doCall(props, message, defaultTimeout);
	}
 
	public Response doCall(AMQP.BasicProperties props, byte[] message, int timeout)
	        throws Exception {
        checkConsumer();
        if(futureMap.size() >= capacity)
			throw new EzyRabbitMaxCapacity("rpc client too many request, capacity: " + capacity);
        String replyId = correlationIdFactory.newCorrelationId();
        AMQP.BasicProperties.Builder propertiesBuilder = (props != null) 
        			? props.builder() 
        			: new AMQP.BasicProperties.Builder();
    	AMQP.BasicProperties newProperties = propertiesBuilder
    			.correlationId(replyId)
    			.replyTo(replyRoutingKey)
    			.build();
    	EzyFuture future = futureMap.addFuture(replyId);
        publish(newProperties, message);
        Object reply;
        try {
        	reply = future.get(timeout);
        } 
        catch (TimeoutException ex) {
        	futureMap.removeFuture(replyId);
            throw ex;
        }
        catch (Exception e) {
        	throw e;
		}
        if (reply instanceof ShutdownSignalException) {
            ShutdownSignalException sig = (ShutdownSignalException) reply;
            ShutdownSignalException wrapper =
                new ShutdownSignalException(sig.isHardError(),
                                            sig.isInitiatedByApplication(),
                                            sig.getReason(),
                                            sig.getReference());
            wrapper.initCause(sig);
            throw wrapper;
        } else {
            return (Response) reply;
        }
	}
	
	protected void publish(AMQP.BasicProperties props, byte[] message) 
			throws IOException {
		channel.basicPublish(exchange, requestRoutingKey, props, message);
	}
	
	public void checkConsumer() throws IOException {
		if (consumer == null) {
            throw new EOFException("RpcClient is closed");
        }
	}
	
	public void close() throws IOException {
        if (consumer != null) {
            channel.basicCancel(consumer.getConsumerTag());
            consumer = null;
        }
    }
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyRabbitEndpoint.Builder<Builder> {
		
		protected int capacity;
		protected int defaultTimeout;
		protected String routingKey; 
		protected String replyQueueName;
		protected String replyRoutingKey; 
		protected EzyRabbitCorrelationIdFactory correlationIdFactory;
		protected EzyRabbitResponseConsumer unconsumedResponseConsumer;
		
		public Builder() {
			this.capacity = 10000;
		}
		
		public Builder capacity(int capacity) {
			this.capacity = capacity;
			return this;
		}
		
		public Builder defaultTimeout(int defaultTimeout) {
			this.defaultTimeout = defaultTimeout;
			return this;
		}
		
		public Builder routingKey(String routingKey) {
			this.routingKey = routingKey;
			return this;
		}
		
		public Builder replyQueueName(String replyQueueName) {
			this.replyQueueName = replyQueueName;
			return this;
		}
		
		public Builder replyRoutingKey(String replyRoutingKey) {
			this.replyRoutingKey = replyRoutingKey;
			return this;
		}
		
		public Builder correlationIdFactory(EzyRabbitCorrelationIdFactory correlationIdFactory) {
			this.correlationIdFactory = correlationIdFactory;
			return this;
		}
		
		public Builder unconsumedResponseConsumer(EzyRabbitResponseConsumer unconsumedResponseConsumer) {
			this.unconsumedResponseConsumer = unconsumedResponseConsumer;
			return this;
		}
		
		@Override
		public EzyRabbitRpcClient build() {
			try {
				if(correlationIdFactory == null)
					correlationIdFactory = new EzyRabbitSimpleCorrelationIdFactory();
				return new EzyRabbitRpcClient(
						channel, 
						exchange, 
						routingKey, 
						replyQueueName, 
						replyRoutingKey,
						capacity,
						defaultTimeout, 
						correlationIdFactory,
						unconsumedResponseConsumer);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		
	}
	
}
