package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.concurrent.EzyFuture;
import com.tvd12.ezyfox.concurrent.EzyFutureConcurrentHashMap;
import com.tvd12.ezyfox.concurrent.EzyFutureMap;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyReturner;
import com.tvd12.ezymq.mosquitto.exception.EzyMosquittoMaxCapacity;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoCorrelationIdFactory;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoSimpleCorrelationIdFactory;
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
    protected final EzyFutureMap<String> futureMap;
    protected final EzyMosquittoCorrelationIdFactory correlationIdFactory;
    protected final EzyMosquittoResponseConsumer unconsumedResponseConsumer;

    public EzyMosquittoRpcClient(
        EzyMqttClientProxy mqttClient,
        String topic,
        String replyTopic,
        int capacity,
        int defaultTimeout,
        EzyMosquittoCorrelationIdFactory correlationIdFactory,
        EzyMosquittoResponseConsumer unconsumedResponseConsumer
    ) {
        super(mqttClient, topic);
        this.capacity = capacity;
        this.defaultTimeout = defaultTimeout;
        this.correlationIdFactory = correlationIdFactory;
        this.futureMap = new EzyFutureConcurrentHashMap<>();
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
        if (replyTopic != null) {
            this.mqttClient.registerCallback(
                replyTopic,
                setupMqttCallback()
            );
        }
    }

    protected EzyMqttCallback setupMqttCallback() {
        return new EzyMqttCallback() {
            @Override
            public void connectionLost(EzyMqttConnectionLostException e) {
                Map<String, EzyFuture> remainFutures = futureMap.clear();
                for (EzyFuture future : remainFutures.values()) {
                    future.setResult(e);
                }
            }

            @Override
            public void messageArrived(
                EzyMosquittoProperties properties,
                byte[] body
            ) {
                String correlationId = properties.getCorrelationId();
                EzyFuture future = futureMap.removeFuture(correlationId);
                if (future == null) {
                    if (unconsumedResponseConsumer != null) {
                        unconsumedResponseConsumer.consume(properties, body);
                    } else {
                        logger.warn(
                            "No outstanding request for message correlation ID {}",
                            correlationId
                        );
                    }
                } else {
                    future.setResult(new EzyMosquittoMessage(properties, body));
                }
            }
        };
    }

    public void doFire(
        EzyMosquittoProperties props,
        byte[] message
    ) throws Exception {
        EzyMosquittoProperties newProperties = props != null
            ? props
            : EzyMosquittoProperties.builder().build();
        publish(newProperties, message);
    }

    public EzyMosquittoMessage doCall(
        EzyMosquittoProperties props,
        byte[] message
    ) throws Exception {
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
        String correlationId = correlationIdFactory
            .newCorrelationId(topic);
        EzyMosquittoProperties.Builder propertiesBuilder = (props != null)
            ? props.toBuilder()
            : new EzyMosquittoProperties.Builder();
        EzyMosquittoProperties newProperties = propertiesBuilder
            .correlationId(correlationId)
            .build();
        EzyFuture future = futureMap.addFuture(correlationId);
        publish(newProperties, message);
        Object reply;
        try {
            reply = future.get(timeout);
        } catch (TimeoutException ex) {
            futureMap.removeFuture(correlationId);
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
        protected String replyTopic;
        protected EzyMosquittoCorrelationIdFactory correlationIdFactory;
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

        public Builder replyTopic(String replyTopic) {
            this.replyTopic = replyTopic;
            return this;
        }

        public Builder correlationIdFactory(
            EzyMosquittoCorrelationIdFactory correlationIdFactory
        ) {
            this.correlationIdFactory = correlationIdFactory;
            return this;
        }

        public Builder unconsumedResponseConsumer(
            EzyMosquittoResponseConsumer unconsumedResponseConsumer
        ) {
            this.unconsumedResponseConsumer = unconsumedResponseConsumer;
            return this;
        }

        @Override
        public EzyMosquittoRpcClient build() {
            if (correlationIdFactory == null) {
                correlationIdFactory = new EzyMosquittoSimpleCorrelationIdFactory();
            }
            return EzyReturner.returnWithException(() ->
                new EzyMosquittoRpcClient(
                    mqttClient,
                    topic,
                    replyTopic,
                    capacity,
                    defaultTimeout,
                    correlationIdFactory,
                    unconsumedResponseConsumer
                )
            );
        }
    }
}
