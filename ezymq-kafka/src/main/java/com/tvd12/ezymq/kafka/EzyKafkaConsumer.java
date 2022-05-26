package com.tvd12.ezymq.kafka;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("rawtypes")
public class EzyKafkaConsumer
    extends EzyLoggable
    implements EzyKafkaRecordsHandler, EzyStartable, EzyCloseable {

    protected final EzyKafkaServer server;
    protected final EzyKafkaDataCodec dataCodec;
    protected final EzyKafkaMessageHandlers messageHandlers;
    @Setter
    protected List<EzyKafkaMessageInterceptor> messageInterceptors;

    protected static final byte[] BINARY_TYPE = new byte[]{(byte) 'b'};

    public EzyKafkaConsumer(
        EzyKafkaServer server,
        EzyKafkaDataCodec dataCodec,
        EzyKafkaMessageHandlers messageHandlers
    ) {
        this.server = server;
        this.server.setRecordsHandler(this);
        this.dataCodec = dataCodec;
        this.messageHandlers = messageHandlers;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        server.start();
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
            for (EzyKafkaMessageInterceptor messageInterceptor : messageInterceptors) {
                messageInterceptor.preHandle(topic, cmd, message);
            }
            result = messageHandlers.handle(cmd, message);
            for (EzyKafkaMessageInterceptor messageInterceptor : messageInterceptors) {
                messageInterceptor.postHandle(topic, cmd, message, result);
            }
        } catch (Throwable e) {
            for (EzyKafkaMessageInterceptor messageInterceptor : messageInterceptors) {
                messageInterceptor.postHandle(topic, cmd, message, e);
            }
        }
    }

    public static class Builder implements EzyBuilder<EzyKafkaConsumer> {

        protected EzyKafkaServer server;
        protected EzyKafkaDataCodec dataCodec;
        protected EzyKafkaMessageHandlers messageHandlers;
        protected List<EzyKafkaMessageInterceptor> messageInterceptors;

        public Builder server(EzyKafkaServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyKafkaDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder messageHandlers(EzyKafkaMessageHandlers messageHandlers) {
            this.messageHandlers = messageHandlers;
            return this;
        }

        public Builder messageInterceptors(List<EzyKafkaMessageInterceptor> messageInterceptors) {
            this.messageInterceptors = messageInterceptors;
            return this;
        }

        @Override
        public EzyKafkaConsumer build() {
            EzyKafkaConsumer handler = new EzyKafkaConsumer(server, dataCodec, messageHandlers);
            handler.setMessageInterceptors(messageInterceptors);
            return handler;
        }
    }
}
