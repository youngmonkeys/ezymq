package com.tvd12.ezymq.kafka.endpoint;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyKafkaClient extends EzyKafkaEndpoint {

	protected final Producer producer;
	
	public EzyKafkaClient(Producer producer) {
		this.producer = producer;
	}
	
	public void send(String cmd, byte[] message) throws Exception {
		ProducerRecord record = new ProducerRecord<>(cmd, message);
		producer.send(record);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyKafkaEndpoint.Builder<Builder> {
		
		protected Producer producer;
		
		public Builder producer(Producer producer) {
			this.producer = producer;
			return this;
		}
		
		@Override
		public EzyKafkaClient build() {
			if(producer == null)
				this.producer = newProducer();
			return new EzyKafkaClient(producer);
		}
		
	}
	
}
