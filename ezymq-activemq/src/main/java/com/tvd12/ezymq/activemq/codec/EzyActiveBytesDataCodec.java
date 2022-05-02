package com.tvd12.ezymq.activemq.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings("rawtypes")
public class EzyActiveBytesDataCodec extends EzyActiveAbstractDataCodec {

    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;

    public EzyActiveBytesDataCodec() {}

    public EzyActiveBytesDataCodec(
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer
    ) {
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
    }

    public EzyActiveBytesDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        Map<String, Class> requestTypeMap
    ) {
        super(marshaller, unmarshaller, requestTypeMap);
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Object deserialize(String cmd, byte[] request) {
        Object data = messageDeserializer.deserialize(request);
        return unmarshallData(cmd, data);
    }

    @Override
    public byte[] serialize(Object response) {
        Object data = marshallEntity(response);
        return messageSerializer.serialize(data);
    }

    public static class Builder extends EzyActiveAbstractDataCodecBuilder<Builder> {
        protected EzyMessageSerializer messageSerializer;
        protected EzyMessageDeserializer messageDeserializer;

        public Builder messageSerializer(EzyMessageSerializer messageSerializer) {
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
            this.messageDeserializer = messageDeserializer;
            return this;
        }

        @Override
        protected EzyActiveAbstractDataCodec newProduct() {
            return new EzyActiveBytesDataCodec(messageSerializer, messageDeserializer);
        }
    }
}
