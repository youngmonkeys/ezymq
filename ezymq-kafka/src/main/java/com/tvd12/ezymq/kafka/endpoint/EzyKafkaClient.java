package com.tvd12.ezymq.kafka.endpoint;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyProcessor;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyKafkaClient 
		extends EzyKafkaEndpoint implements EzyCloseable {

	protected final Producer producer;
	
	public EzyKafkaClient(String topic, Producer producer) {
		super(topic);
		this.producer = producer;
	}
	
	public void send(String cmd, byte[] message) throws Exception {
		ProducerRecord record = null;
		if(topic == null)
			record = new ProducerRecord<>(cmd, message);
		else 
			record = new ProducerRecord<>(topic, cmd, message);
		producer.send(record);
	}
	
	@Override
	public void close() {
		EzyProcessor.processWithLogException(() -> producer.close());
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyKafkaEndpoint.Builder<Builder> {
		
		protected Producer producer;
		protected Serializer serializer;
		
		public Builder producer(Producer producer) {
			this.producer = producer;
			return this;
		}
		
		public Builder serializer(Serializer serializer) {
			this.serializer = serializer;
			return this;
		}
		
		@Override
		public EzyKafkaClient build() {
			if(producer == null)
				this.producer = newProducer(serializer);
			return new EzyKafkaClient(topic, producer);
		}
		
	}
	
}
