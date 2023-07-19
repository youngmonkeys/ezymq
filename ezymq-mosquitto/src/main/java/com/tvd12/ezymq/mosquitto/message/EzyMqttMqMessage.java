package com.tvd12.ezymq.mosquitto.message;

import lombok.Getter;

import java.util.Map;

@Getter
public class EzyMqttMqMessage {
    private final int id;
    private final String type;
    private final String correlationId;
    private final int qos;
    private final boolean retained;
    private final Map<String, Object> headers;
    private final byte[] body;

    private EzyMqttMqMessage(Builder builder) {
        this.id = builder.id;
        this.type = builder.type;
        this.correlationId = builder.correlationId;
        this.qos = builder.qos;
        this.retained = builder.retained;
        this.headers = builder.headers;
        this.body = builder.body;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int id;
        private String type;
        private String correlationId;
        private int qos = 1;
        private boolean retained;
        private Map<String, Object> headers;
        private byte[] body;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder qos(int qos) {
            this.qos = qos;
            return this;
        }

        public Builder retained(boolean retained) {
            this.retained = retained;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body;
            return this;
        }

        public EzyMqttMqMessage build() {
            return new EzyMqttMqMessage(this);
        }
    }
}

