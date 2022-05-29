package com.tvd12.ezymq.common.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQAbstractDataCodec implements EzyMQDataCodec {

    protected final EzyMarshaller marshaller;
    protected final EzyUnmarshaller unmarshaller;
    protected final Map<String, Class> requestTypeMap;

    public EzyMQAbstractDataCodec(
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

    public abstract static class Builder<B extends Builder>
        implements EzyBuilder<EzyMQDataCodec> {

        protected EzyMarshaller marshaller;
        protected EzyUnmarshaller unmarshaller;
        protected final Map<String, Class> requestTypeByCommand =
            new HashMap<>();

        public B marshaller(EzyMarshaller marshaller) {
            this.marshaller = marshaller;
            return (B) this;
        }

        public B unmarshaller(EzyUnmarshaller unmarshaller) {
            this.unmarshaller = unmarshaller;
            return (B) this;
        }

        public B mapRequestType(String cmd, Class requestType) {
            this.requestTypeByCommand.put(cmd, requestType);
            return (B) this;
        }

        public B mapRequestTypes(Map<String, Class> requestTypes) {
            this.requestTypeByCommand.putAll(requestTypes);
            return (B) this;
        }
    }
}
