package com.tvd12.ezymq.activemq.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyActiveAbstractDataCodec implements EzyActiveDataCodec {

    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected Map<String, Class> requestTypeMap;

    public EzyActiveAbstractDataCodec() {}

    public EzyActiveAbstractDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        Map<String, Class> requestTypeMap
    ) {
        this.marshaller = marshaller;
        this.unmarshaller = unmarshaller;
        this.requestTypeMap = requestTypeMap;
    }

    protected Object marshallEntity(Object entity) {
        return marshaller.marshal(entity);
    }

    protected Object unmarshallData(String cmd, Object value) {
        Class requestType = requestTypeMap.get(cmd);
        if (requestType == null) {
            throw new IllegalArgumentException("has no request type with command: " + cmd);
        }
        return unmarshaller.unmarshal(value, requestType);
    }
}
