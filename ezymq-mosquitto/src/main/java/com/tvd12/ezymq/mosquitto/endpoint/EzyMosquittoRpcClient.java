package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.concurrent.EzyFuture;
import com.tvd12.ezyfox.concurrent.EzyFutureConcurrentHashMap;
import com.tvd12.ezyfox.concurrent.EzyFutureMap;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyReturner;
import com.tvd12.ezymq.mosquitto.exception.EzyMosquittoMaxCapacity;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoMessageIdFactory;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoSimpleMessageIdFactory;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoResponseConsumer;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMqttMqMessage;

public class EzyMosquittoRpcClient
    extends EzyMosquittoEndpoint
    implements EzyCloseable {

    protected final int capacity;
    protected final int defaultTimeout;
    protected final EzyFutureMap<Integer> futureMap;
    protected final EzyMosquittoMessageIdFactory messageIdFactory;
    protected final EzyMosquittoResponseConsumer unconsumedResponseConsumer;

    public EzyMosquittoRpcClient(
        EzyMqttClientProxy mqttClient,
        String topic,
        int capacity,
        int defaultTimeout,
        EzyMosquittoMessageIdFactory messageIdFactory,
        EzyMosquittoResponseConsumer unconsumedResponseConsumer
    ) {
        super(mqttClient, topic);
        this.capacity = capacity;
        this.defaultTimeout = defaultTimeout;
        this.messageIdFactory = messageIdFactory;
        this.futureMap = new EzyFutureConcurrentHashMap<>();
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
        this.mqttClient.registerCallback(
            topic,
            setupMqttCallback()
        );
    }

    protected EzyMqttCallback setupMqttCallback() {
        return new EzyMqttCallback() {
            @Override
            public void connectionLost(EzyMqttConnectionLostException e) {
                Map<Integer, EzyFuture> remainFutures = futureMap.clear();
                for (EzyFuture future : remainFutures.values()) {
                    future.setResult(e);
                }
            }

            @Override
            public void messageArrived(
                EzyMosquittoProperties properties,
                byte[] body
            ) {
                int messageId = properties.getMessageId();
                EzyFuture future = futureMap.removeFuture(messageId);
                if (future == null) {
                    if (unconsumedResponseConsumer != null) {
                        unconsumedResponseConsumer.consume(properties, body);
                    } else {
                        logger.warn("No outstanding request for message ID {}", messageId);
                    }
                } else {
                    future.setResult(new EzyMosquittoMessage(properties, body));
                }
            }
        };
    }

    public void doFire(EzyMosquittoProperties props, byte[] message) throws Exception {
        EzyMosquittoProperties newProperties = props != null
            ? props
            : EzyMosquittoProperties.builder().build();
        publish(newProperties, message);
    }

    public EzyMosquittoMessage doCall(EzyMosquittoProperties props, byte[] message)
        throws Exception {
        return doCall(props, message, defaultTimeout);
    }

    public EzyMosquittoMessage doCall(
        EzyMosquittoProperties props,
        byte[] message,
        int timeout
    ) throws Exception {
        if (futureMap.size() >= capacity) {
            throw new EzyMosquittoMaxCapacity(
                "rpc client too many request, capacity: " + capacity
            );
        }
        int messageId = messageIdFactory.newMessageId(topic);
        EzyMosquittoProperties.Builder propertiesBuilder = (props != null)
            ? props.toBuilder()
            : new EzyMosquittoProperties.Builder();
        EzyMosquittoProperties newProperties = propertiesBuilder
            .messageId(messageId)
            .build();
        EzyFuture future = futureMap.addFuture(messageId);
        publish(newProperties, message);
        Object reply;
        try {
            reply = future.get(timeout);
        } catch (TimeoutException ex) {
            futureMap.removeFuture(messageId);
            throw ex;
        }
        if (reply instanceof Exception) {
            throw (Exception) reply;
        } else {
            return (EzyMosquittoMessage) reply;
        }
    }

    protected void publish(
        EzyMosquittoProperties props,
        byte[] message
    ) throws Exception {
        mqttClient.publish(topic, toMqttMqMessage(props, message));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMosquittoEndpoint.Builder<Builder> {

        protected int capacity;
        protected int defaultTimeout;
        protected EzyMosquittoMessageIdFactory messageIdFactory;
        protected EzyMosquittoResponseConsumer unconsumedResponseConsumer;

        public Builder() {
            this.capacity = 10000;
        }

        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder defaultTimeout(int defaultTimeout) {
            this.defaultTimeout = defaultTimeout;
            return this;
        }

        public Builder messageIdFactory(EzyMosquittoMessageIdFactory messageIdFactory) {
            this.messageIdFactory = messageIdFactory;
            return this;
        }

        public Builder unconsumedResponseConsumer(EzyMosquittoResponseConsumer unconsumedResponseConsumer) {
            this.unconsumedResponseConsumer = unconsumedResponseConsumer;
            return this;
        }

        @Override
        public EzyMosquittoRpcClient build() {
            if (messageIdFactory == null) {
                messageIdFactory = new EzyMosquittoSimpleMessageIdFactory();
            }
            return EzyReturner.returnWithException(() ->
                new EzyMosquittoRpcClient(
                    mqttClient,
                    topic,
                    capacity,
                    defaultTimeout,
                    messageIdFactory,
                    unconsumedResponseConsumer
                )
            );
        }
    }
}
