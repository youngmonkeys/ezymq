package com.tvd12.ezymq.mosquitto.endpoint;

import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMessage;
import static com.tvd12.ezymq.mosquitto.util.EzyMqttMessages.toMqttMessage;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyReturner;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRpcCallHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import lombok.Setter;

public class EzyMosquittoRpcServer
    extends EzyMosquittoEndpoint
    implements EzyStartable, EzyCloseable {

    protected final AtomicBoolean started;
    protected final AtomicInteger startCount;
    protected final EzyMosquittoBufferConsumer consumer;
    protected volatile boolean active;
    @Setter
    protected EzyMosquittoRpcCallHandler callHandler;

    public EzyMosquittoRpcServer(
        MqttClient mqttClient,
        String topic,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        super(mqttClient, topic);
        this.started = new AtomicBoolean();
        this.startCount = new AtomicInteger();
        this.consumer = new EzyMosquittoBufferConsumer();
        mqttCallbackProxy.registerCallback(topic, setupMqttCallback());
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
                    toMessage(rpcTopic, properties, body)
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

    public void processRequest(EzyMosquittoMessage request)
        throws Exception {
        EzyMosquittoProperties requestProperties = request.getProperties();
        int messageId = requestProperties.getMessageId();
        if (messageId > 0) {
            EzyMosquittoProperties.Builder replyPropertiesBuilder =
                new EzyMosquittoProperties.Builder();
            byte[] replyBody = handleCall(request, replyPropertiesBuilder);
            replyPropertiesBuilder
                .messageId(messageId)
                .messageType(requestProperties.getMessageType());
            EzyMosquittoProperties replyProperties = replyPropertiesBuilder.build();
            MqttMessage mqttMessage = toMqttMessage(rpcTopic, replyProperties, replyBody);
            mqttClient.publish(topic, mqttMessage);
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
        protected MqttClient mqttClient;
        protected String topic = "";
        protected EzyMqttCallbackProxy mqttCallbackProxy;

        public Builder mqttClient(MqttClient mqttClient) {
            this.mqttClient = mqttClient;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder mqttCallbackProxy(EzyMqttCallbackProxy mqttCallbackProxy) {
            this.mqttCallbackProxy = mqttCallbackProxy;
            return this;
        }

        @Override
        public EzyMosquittoRpcServer build() {
            return EzyReturner.returnWithException(() ->
                new EzyMosquittoRpcServer(
                    mqttClient,
                    topic,
                    mqttCallbackProxy
                )
            );
        }
    }
}
