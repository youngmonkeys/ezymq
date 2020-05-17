package com.tvd12.ezymq.rabbitmq.codec;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyRabbitAbstractDataCodecBuilder<B extends EzyRabbitAbstractDataCodecBuilder> 
		implements EzyBuilder<EzyRabbitDataCodec> {

	protected EzyMarshaller marshaller;
	protected EzyUnmarshaller unmarshaller;
	protected Map<String, Class> requestTypeMap = new HashMap<>();
	
	public B marshaller(EzyMarshaller marshaller) {
		this.marshaller = marshaller;
		return (B)this;
	}
	
	public B unmarshaller(EzyUnmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
		return (B)this;
	}
	
	public B mapRequestType(String cmd, Class requestType) {
		this.requestTypeMap.put(cmd, requestType);
		return (B)this;
	}
	
	public B mapRequestTypes(Map<String, Class> requestTypes) {
		this.requestTypeMap.putAll(requestTypes);
		return (B)this;
	}
	
	@Override
	public EzyRabbitDataCodec build() {
		EzyRabbitAbstractDataCodec product = newProduct();
		product.setMarshaller(marshaller);
		product.setUnmarshaller(unmarshaller);
		product.setRequestTypeMap(requestTypeMap);
		return product;
	}
	
	protected abstract EzyRabbitAbstractDataCodec newProduct();
	
}
