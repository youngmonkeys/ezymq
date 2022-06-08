package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumers;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;

import java.util.List;
import java.util.Map;

public class EzyRabbitTopic<T> implements EzyCloseable {

    protected final String name;
    protected volatile boolean consuming;
    protected final EzyRabbitTopicClient client;
    protected final EzyRabbitTopicServer server;
    protected final EzyMQDataCodec dataCodec;
    protected final EzyMQMessageConsumers consumers;

    public EzyRabbitTopic(
        String name,
        EzyMQDataCodec dataCodec,
        EzyRabbitTopicClient client,
        EzyRabbitTopicServer server
    ) {
        this.name = name;
        this.client = client;
        this.server = server;
        this.dataCodec = dataCodec;
        this.consumers = new EzyMQMessageConsumers();
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
                "this topic is consuming only, " +
                    "must enable producer (.producerEnable(true)) in the settings"
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
                "this topic is publishing only, " +
                    "must enable consumer (.consumerEnable(true)) in the settings"
            );
        }
        synchronized (this) {
            boolean needStartConsuming = false;
            if (!consuming) {
                this.consuming = true;
                needStartConsuming = true;
            }
            consumers.addConsumer(cmd, consumer);
            if (needStartConsuming) {
                this.startConsuming();
            }
        }
    }

    public void addConsumers(
        Map<String, List<EzyMQMessageConsumer<T>>> consumersMap
    ) {
        if (consumersMap != null) {
            for (String cmd : consumersMap.keySet()) {
                for (EzyMQMessageConsumer<T> consumer : consumersMap.get(cmd)) {
                    addConsumer(cmd, consumer);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void startConsuming() {
        EzyRabbitMessageHandler messageHandler = (requestProperties, requestBody) -> {
            String cmd = requestProperties.getType();
            if (EzyStrings.isNoContent(cmd)) {
                cmd = "";
            }
            T message = (T) dataCodec.deserializeTopicMessage(name, cmd, requestBody);
            consumers.consume(cmd, message);
        };
        server.setMessageHandler(messageHandler);
        try {
            server.start();
        } catch (Exception e) {
            throw new IllegalStateException("can't start topic server", e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @SuppressWarnings("rawtypes")
    public static class Builder implements EzyBuilder<EzyRabbitTopic> {

        protected String name;
        protected EzyMQDataCodec dataCodec;
        protected EzyRabbitTopicClient client;
        protected EzyRabbitTopicServer server;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

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
            return new EzyRabbitTopic(name, dataCodec, client, server);
        }
    }
}
