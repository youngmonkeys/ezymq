package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezyfox.builder.EzyBuilder;
import lombok.Getter;

import java.util.Map;

@Getter
public class EzyMosquittoProperties {

    protected final int messageId;
    protected final String messageType;
    protected final String correlationId;
    protected final int qos;
    protected final boolean retained;
    protected final Map<String, Object> headers;

    protected EzyMosquittoProperties(Builder builder) {
        this.qos = builder.qos;
        this.retained = builder.retained;
        this.messageId = builder.messageId;
        this.messageType = builder.messageType;
        this.correlationId = builder.correlationId;
        this.headers = builder.headers;
    }

    public Builder toBuilder() {
        return builder()
            .messageId(messageId)
            .messageType(messageType)
            .correlationId(correlationId)
            .qos(qos)
            .retained(retained);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "(" +
            "messageId: " + messageId + ", " +
            "messageType: " + messageType + ", " +
            "correlationId: " + correlationId + ", " +
            "qos: " + qos + ", " +
            "retained: " + retained + ", " +
            "headers: " + headers + ", " +
            ")";
    }

    public static class Builder
        implements EzyBuilder<EzyMosquittoProperties> {

        protected int messageId;
        protected String correlationId;
        protected String messageType = "";
        protected int qos = 1;
        protected boolean retained;
        protected Map<String, Object> headers;

        public Builder qos(int qos) {
            this.qos = qos;
            return this;
        }

        public Builder retained(boolean retained) {
            this.retained = retained;
            return this;
        }

        public Builder messageId(int messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder messageType(String messageType) {
            this.messageType = messageType;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        @Override
        public EzyMosquittoProperties build() {
            return new EzyMosquittoProperties(this);
        }
    }
}
