package com.tvd12.ezymq.activemq.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyActiveAbstractDataCodecBuilder<B extends EzyActiveAbstractDataCodecBuilder>
    implements EzyBuilder<EzyActiveDataCodec> {

    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected Map<String, Class> requestTypeMap = new HashMap<>();

    public B marshaller(EzyMarshaller marshaller) {
        this.marshaller = marshaller;
        return (B) this;
    }

    public B unmarshaller(EzyUnmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
        return (B) this;
    }

    public B mapRequestType(String cmd, Class requestType) {
        this.requestTypeMap.put(cmd, requestType);
        return (B) this;
    }

    public B mapRequestTypes(Map<String, Class> requestTypes) {
        this.requestTypeMap.putAll(requestTypes);
        return (B) this;
    }

    @Override
    public EzyActiveDataCodec build() {
        EzyActiveAbstractDataCodec product = newProduct();
        product.setMarshaller(marshaller);
        product.setUnmarshaller(unmarshaller);
        product.setRequestTypeMap(requestTypeMap);
        return product;
    }

    protected abstract EzyActiveAbstractDataCodec newProduct();
}
