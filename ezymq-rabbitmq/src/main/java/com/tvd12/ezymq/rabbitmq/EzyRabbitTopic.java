package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumers;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;

public class EzyRabbitTopic<T> {

    protected final EzyRabbitTopicClient client;
    protected final EzyRabbitTopicServer server;
    protected final EzyMQDataCodec dataCodec;
    protected volatile boolean consuming;
    protected EzyMQMessageConsumers consumers;

    public EzyRabbitTopic(
        EzyMQDataCodec dataCodec,
        EzyRabbitTopicClient client,
        EzyRabbitTopicServer server
    ) {
        this.client = client;
        this.server = server;
        this.dataCodec = dataCodec;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void publish(Object data) {
        String cmd = "";
        if (data instanceof EzyMessageTypeFetcher) {
            cmd = ((EzyMessageTypeFetcher) data).getMessageType();
        }
        publish(cmd, data);
    }

    public void publish(String cmd, Object data) {
        if (client == null) {
            throw new IllegalStateException(
                "this topic is consuming only, set the client to publish"
            );
        }
        BasicProperties requestProperties = new BasicProperties.Builder()
            .type(cmd)
            .build();
        byte[] requestMessage = dataCodec.serialize(data);
        rawPublish(requestProperties, requestMessage);
    }

    protected void rawPublish(
        BasicProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            client.publish(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public void addConsumer(EzyMQMessageConsumer<T> consumer) {
        addConsumer("", consumer);
    }

    public void addConsumer(String cmd, EzyMQMessageConsumer<T> consumer) {
        if (server == null) {
            throw new IllegalStateException(
                "this topic is publishing only, set the server to consume"
            );
        }
        synchronized (this) {
            if (!consuming) {
                this.consuming = true;
                this.consumers = new EzyMQMessageConsumers();
                this.startConsuming();
            }
            consumers.addConsumer(cmd, consumer);
        }
    }

    @SuppressWarnings("unchecked")
    protected void startConsuming() {
        EzyRabbitMessageHandler messageHandler = (requestProperties, requestBody) -> {
            String cmd = requestProperties.getType();
            if (EzyStrings.isNoContent(cmd)) {
                cmd = "";
            }
            T message = (T) dataCodec.deserialize(cmd, requestBody);
            consumers.consume(cmd, message);
        };
        server.setMessageHandler(messageHandler);
        try {
            server.start();
        } catch (Exception e) {
            throw new IllegalStateException("can't start topic server", e);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class Builder implements EzyBuilder<EzyRabbitTopic> {

        protected EzyRabbitTopicClient client;
        protected EzyRabbitTopicServer server;
        protected EzyMQDataCodec dataCodec;

        public Builder client(EzyRabbitTopicClient client) {
            this.client = client;
            return this;
        }

        public Builder server(EzyRabbitTopicServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyMQDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public EzyRabbitTopic build() {
            return new EzyRabbitTopic(dataCodec, client, server);
        }
    }
}
