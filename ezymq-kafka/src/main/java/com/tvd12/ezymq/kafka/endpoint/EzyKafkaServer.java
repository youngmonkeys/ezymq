package com.tvd12.ezymq.kafka.endpoint;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;

import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyProcessor;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class EzyKafkaServer 
		extends EzyKafkaEndpoint
		implements EzyStartable, EzyCloseable {
	
	protected final long pollTimeOut;
	protected final Consumer consumer;
	protected volatile boolean active;
	protected final EzyThreadList executorService;
	
	@Setter
	protected EzyKafkaRecordsHandler recordsHandler;
	
	public EzyKafkaServer(
			String topic, 
			Consumer consumer, 
			long poolTimeOut, int threadPoolSize) {
		this(
			topic, 
			consumer, 
			poolTimeOut, 
			newExecutorServiceSupplier(threadPoolSize)
		);
	}
	
	public EzyKafkaServer(
			String topic,
			Consumer consumer, 
			long poolTimeOut, 
			Function<Runnable, EzyThreadList> executorServiceSupplier) {
		super(topic);
		this.consumer = consumer;
		this.pollTimeOut = poolTimeOut;
		this.executorService = newExecutorService(executorServiceSupplier);
	}
	
	protected EzyThreadList newExecutorService(
			Function<Runnable, EzyThreadList> executorServiceSupplier) {
		EzyThreadList answer = null;
		if(executorServiceSupplier != null)
			answer = executorServiceSupplier.apply(() -> loop());
		return answer;
	}
	
	protected static Function<Runnable, EzyThreadList> newExecutorServiceSupplier(
			int threadPoolSize) {
		if(threadPoolSize <= 0)
			return null;
		return t -> new EzyThreadList(threadPoolSize, t, "kafka-server");
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void start() throws Exception {
		this.active = true;
		this.consumer.subscribe(Collections.singletonList(topic));
		if(executorService == null)
			loop();
		else
			executorService.execute();
	}
	
	protected void loop() {
		while(active)
			pollRecords();
	}
	
	protected void pollRecords() {
		try {
			ConsumerRecords records = ConsumerRecords.EMPTY;
			synchronized (this) {
				records = consumer.poll(Duration.ofMillis(pollTimeOut));
			}
			recordsHandler.handleRecords(records);
		}
		catch(Exception e) {
			if(active)
				logger.warn("poll and handle records error", e);
		}
	}
	
	@Override
	public void close() {
		this.active = false;
		synchronized (this) {
			EzyProcessor.processWithLogException(() -> consumer.close());
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyKafkaEndpoint.Builder<Builder> {
		
		protected Consumer consumer;
		protected int threadPoolSize;
		protected long pollTimeOut = 100;
		protected Deserializer deserializer;
		protected EzyKafkaRecordsHandler recordsHandler;
		
		public Builder pollTimeOut(long pollTimeOut) {
			this.pollTimeOut = pollTimeOut;
			return this;
		}
		
		public Builder threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}
		
		public Builder consumer(Consumer consumer) {
			this.consumer = consumer;
			return this;
		}
		
		public Builder deserializer(Deserializer deserializer) {
			this.deserializer = deserializer;
			return this;
		}
		
		public Builder recordsHandler(EzyKafkaRecordsHandler recordsHandler) {
			this.recordsHandler = recordsHandler;
			return this;
		}
		
		@Override
		public EzyKafkaServer build() {
			if(consumer == null)
				this.consumer = newConsumer(deserializer);
			return new EzyKafkaServer(topic, consumer, pollTimeOut, threadPoolSize);
		}
		
	}
	
}
