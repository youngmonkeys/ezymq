package com.tvd12.ezymq.mosquitto.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tvd12.ezyfox.builder.EzyBuilder;

import lombok.Getter;

@Getter
public class EzyMosquittoProperties {

    protected final int messageId;
    protected final String messageType;
    protected final int qos;
    protected final boolean retained;
    protected final Map<String, Object> headers;

    protected EzyMosquittoProperties(Builder builder) {
        this.qos = builder.qos;
        this.retained = builder.retained;
        this.messageId = builder.messageId;
        this.messageType = builder.messageType;
        this.headers = builder.headers;
    }

    public Builder toBuilder() {
        return builder()
            .messageId(messageId)
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
            "qos: " + qos + ", " +
            "retained: " + retained +
            ")";
    }

    public static class Builder
        implements EzyBuilder<EzyMosquittoProperties> {

        protected int messageId;
        protected String messageType = "";
        protected int qos = 1;
        protected boolean retained;
        protected final Map<String, Object> headers = new HashMap<>();

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

        public Builder headers(Map<String, Object> headers) {
            this.headers.putAll(headers);
            return this;
        }

        @Override
        public EzyMosquittoProperties build() {
            return new EzyMosquittoProperties(this);
        }
    }
}
