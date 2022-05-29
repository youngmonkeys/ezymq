package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageConsumer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageConsumers;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

public class EzyActiveTopic<T> implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final EzyActiveTopicClient client;
    protected final EzyActiveTopicServer server;
    protected volatile boolean consuming;
    protected EzyActiveMessageConsumers consumers;

    public EzyActiveTopic(
        EzyMQDataCodec dataCodec,
        EzyActiveTopicClient client,
        EzyActiveTopicServer server
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
            throw new IllegalStateException("this topic is consuming only, set the client to publish");
        }
        EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
            .type(cmd)
            .build();
        byte[] requestMessage = dataCodec.serialize(data);
        rawPublish(requestProperties, requestMessage);
    }

    protected void rawPublish(
        EzyActiveProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            client.publish(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public void addConsumer(EzyActiveMessageConsumer<T> consumer) {
        addConsumer("", consumer);
    }

    public void addConsumer(String cmd, EzyActiveMessageConsumer<T> consumer) {
        if (server == null) {
            throw new IllegalStateException("this topic is publishing only, set the server to consume");
        }
        synchronized (this) {
            if (!consuming) {
                this.consuming = true;
                this.consumers = new EzyActiveMessageConsumers();
                this.startConsuming();
            }
            consumers.addConsumer(cmd, consumer);
        }
    }

    @SuppressWarnings("unchecked")
    protected void startConsuming() {
        EzyActiveMessageHandler messageHandler = (requestProperties, requestBody) -> {
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
            throw new IllegalStateException("can't start topic server");
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
    public static class Builder implements EzyBuilder<EzyActiveTopic> {

        protected EzyMQDataCodec dataCodec;
        protected EzyActiveTopicClient client;
        protected EzyActiveTopicServer server;

        public Builder dataCodec(EzyMQDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder client(EzyActiveTopicClient client) {
            this.client = client;
            return this;
        }

        public Builder server(EzyActiveTopicServer server) {
            this.server = server;
            return this;
        }

        public EzyActiveTopic build() {
            return new EzyActiveTopic(dataCodec, client, server);
        }
    }
}
