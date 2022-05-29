package com.tvd12.ezymq.activemq.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings("rawtypes")
public class EzyActiveBytesDataCodec
    extends EzyMQBytesDataCodec
    implements EzyActiveDataCodec {

    public EzyActiveBytesDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        Map<String, Class> requestTypeMap
    ) {
        super(
            marshaller,
            unmarshaller,
            messageSerializer,
            messageDeserializer,
            requestTypeMap
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMQBytesDataCodec.Builder {

        @Override
        public EzyActiveBytesDataCodec build() {
            return new EzyActiveBytesDataCodec(
                marshaller,
                unmarshaller,
                messageSerializer,
                messageDeserializer,
                requestTypeByCommand
            );
        }
    }
}
