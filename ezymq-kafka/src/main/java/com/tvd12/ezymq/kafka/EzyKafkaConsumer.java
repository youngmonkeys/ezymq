package com.tvd12.ezymq.kafka;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptors;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Arrays;

@SuppressWarnings("rawtypes")
public class EzyKafkaConsumer
    extends EzyLoggable
    implements EzyKafkaRecordsHandler, EzyCloseable {

    protected final EzyKafkaServer server;
    protected final EzyKafkaDataCodec dataCodec;
    protected final EzyKafkaMessageHandlers messageHandlers;
    protected final EzyKafkaMessageInterceptors messageInterceptors;

    protected static final byte[] BINARY_TYPE = new byte[]{(byte) 'b'};

    public EzyKafkaConsumer(
        EzyKafkaServer server,
        EzyKafkaDataCodec dataCodec,
        EzyKafkaMessageHandlers messageHandlers,
        EzyKafkaMessageInterceptors messageInterceptors
    ) {
        this.dataCodec = dataCodec;
        this.messageHandlers = messageHandlers;
        this.messageInterceptors = messageInterceptors;
        this.server = server;
        this.server.setRecordsHandler(this);
        this.server.start();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() {
        server.close();
    }

    @Override
    public void handleRecord(ConsumerRecord record) {
        String cmd = "";
        String topic = record.topic();
        Object key = record.key();
        if (key != null) {
            cmd = new String((byte[]) key);
        }
        Header contentTypeHeader = record.headers().lastHeader("c");

        Object message = null;
        Object result;
        try {
            byte[] requestBody = (byte[]) record.value();
            if (contentTypeHeader == null) {
                message = dataCodec.deserialize(topic, cmd, requestBody);
            } else {
                byte[] contentTypeBytes = contentTypeHeader.value();
                if (Arrays.equals(contentTypeBytes, BINARY_TYPE)) {
                    message = dataCodec.deserialize(topic, cmd, requestBody);
                } else {
                    message = dataCodec.deserializeText(topic, cmd, requestBody);
                }
            }
            messageInterceptors.preHandle(topic, cmd, message);
            result = messageHandlers.handle(cmd, message);
            messageInterceptors.postHandle(topic, cmd, message, result);
        } catch (Throwable e) {
            messageInterceptors.postHandle(topic, cmd, message, e);
        }
    }

    public static class Builder implements EzyBuilder<EzyKafkaConsumer> {

        protected EzyKafkaServer server;
        protected EzyKafkaDataCodec dataCodec;
        protected EzyKafkaMessageHandlers messageHandlers;
        protected EzyKafkaMessageInterceptors messageInterceptors;

        public Builder server(EzyKafkaServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyKafkaDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder messageHandlers(
            EzyKafkaMessageHandlers messageHandlers
        ) {
            this.messageHandlers = messageHandlers;
            return this;
        }

        public Builder messageInterceptors(
            EzyKafkaMessageInterceptors messageInterceptors
        ) {
            this.messageInterceptors = messageInterceptors;
            return this;
        }

        @Override
        public EzyKafkaConsumer build() {
            return new EzyKafkaConsumer(
                server,
                dataCodec,
                messageHandlers,
                messageInterceptors
            );
        }
    }
}
