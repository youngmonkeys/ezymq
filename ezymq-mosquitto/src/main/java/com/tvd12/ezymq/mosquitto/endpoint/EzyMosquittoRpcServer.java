package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyReturner;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRpcCallHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import lombok.Setter;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMqttMqMessage;

public class EzyMosquittoRpcServer
    extends EzyMosquittoEndpoint
    implements EzyStartable, EzyCloseable {

    protected final String replyTopic;
    protected final AtomicBoolean started;
    protected final AtomicInteger startCount;
    protected final EzyMosquittoBufferConsumer consumer;
    protected volatile boolean active;
    @Setter
    protected EzyMosquittoRpcCallHandler callHandler;

    public EzyMosquittoRpcServer(
        EzyMqttClientProxy mqttClient,
        String topic,
        String replyTopic
    ) {
        super(mqttClient, topic);
        this.replyTopic = replyTopic;
        this.started = new AtomicBoolean();
        this.startCount = new AtomicInteger();
        this.consumer = new EzyMosquittoBufferConsumer();
        this.mqttClient.registerCallback(topic, setupMqttCallback());
    }

    protected EzyMqttCallback setupMqttCallback() {
        return new EzyMqttCallback() {
            @Override
            public void connectionLost(EzyMqttConnectionLostException e) {
                consumer.handleShutdownSignal(e);
            }

            @Override
            public void messageArrived(
                EzyMosquittoProperties properties,
                byte[] body
            ) {
                consumer.handleDelivery(
                    new EzyMosquittoMessage(
                        properties,
                        body
                    )
                );
            }
        };
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            this.active = true;
            this.startCount.incrementAndGet();
            while (active) {
                handleRequestOne();
            }
        } else {
            throw new IllegalStateException("start's already started");
        }
    }

    protected void handleRequestOne() {
        EzyMosquittoMessage request = null;
        try {
            request = consumer.nextDelivery();
            if (request != null) {
                processRequest(request);
            } else {
                active = false;
            }
        } catch (Exception e) {
            if (e instanceof CancellationException) {
                this.active = false;
                logger.info("rpc server by request queue: {} has cancelled", topic, e);
            } else if (e instanceof EzyMqttConnectionLostException) {
                this.active = false;
                logger.info("rpc server by request queue: {} has shutdown", topic, e);
            } else {
                logger.warn("process request: {} of queue: {} error", request, topic, e);
            }
        }
    }

    public void processRequest(EzyMosquittoMessage request) throws Exception {
        EzyMosquittoProperties requestProperties = request.getProperties();
        String correlationId = requestProperties.getCorrelationId();
        if (replyTopic != null && correlationId != null) {
            EzyMosquittoProperties.Builder replyPropertiesBuilder =
                new EzyMosquittoProperties.Builder();
            byte[] replyBody = handleCall(request, replyPropertiesBuilder);
            EzyMosquittoProperties replyProperties = replyPropertiesBuilder
                .messageId(requestProperties.getMessageId())
                .messageType(requestProperties.getMessageType())
                .correlationId(correlationId)
                .build();
            mqttClient.publish(replyTopic, toMqttMqMessage(replyProperties, replyBody));
        } else {
            handleFire(request);
        }
    }

    protected void handleFire(EzyMosquittoMessage request) {
        callHandler.handleFire(request);
    }

    protected byte[] handleCall(
        EzyMosquittoMessage request,
        EzyMosquittoProperties.Builder replyPropertiesBuilder
    ) {
        return callHandler.handleCall(request, replyPropertiesBuilder);
    }

    @Override
    public void close() {
        this.active = false;
        this.callHandler = null;
        for (int i = 0; i < startCount.get(); ++i) {
            this.consumer.close();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyMosquittoRpcServer> {
        protected EzyMqttClientProxy mqttClient;
        protected String topic = "";
        protected String replyTopic;

        public Builder mqttClient(EzyMqttClientProxy mqttClient) {
            this.mqttClient = mqttClient;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder replyTopic(String replyTopic) {
            this.replyTopic = replyTopic;
            return this;
        }

        @Override
        public EzyMosquittoRpcServer build() {
            return EzyReturner.returnWithException(() ->
                new EzyMosquittoRpcServer(
                    mqttClient,
                    topic,
                    replyTopic
                )
            );
        }
    }
}
