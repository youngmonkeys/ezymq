package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyProcessor;
import com.tvd12.ezymq.activemq.concurrent.EzyActiveThreadFactory;
import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.ThreadFactory;

public abstract class EzyActiveRpcEndpoint
    extends EzyActiveEndpoint implements EzyCloseable {

    protected final int threadPoolSize;
    protected final Destination requestQueue;
    protected final Destination replyQueue;
    protected final MessageProducer producer;
    protected final MessageConsumer consumer;
    protected final EzyThreadList executorService;
    protected volatile boolean active;

    public EzyActiveRpcEndpoint(
        Session session,
        Destination requestQueue,
        Destination replyQueue,
        int threadPoolSize
    ) throws Exception {
        super(session);
        this.requestQueue = requestQueue;
        this.replyQueue = replyQueue;
        this.threadPoolSize = threadPoolSize;
        this.producer = createProducer();
        this.consumer = createConsumer();
        this.executorService = newExecutorService();
    }

    protected abstract MessageProducer createProducer() throws Exception;

    protected abstract MessageConsumer createConsumer() throws Exception;

    protected EzyThreadList newExecutorService() {
        ThreadFactory threadFactory
            = EzyActiveThreadFactory.create(getThreadName());
        return new EzyThreadList(threadPoolSize, this::loop, threadFactory);
    }

    protected final void loop() {
        while (active) {
            handleLoopOne();
        }
    }

    protected abstract void handleLoopOne();

    protected void publish(
        EzyActiveProperties props,
        byte[] message
    ) throws Exception {
        publish(producer, props, message);
    }

    public void close() {
        this.active = false;
        EzyProcessor.processWithLogException(producer::close);
        EzyProcessor.processWithLogException(consumer::close);
    }

    protected abstract String getThreadName();

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        extends EzyActiveEndpoint.Builder<B> {

        protected int threadPoolSize = 3;
        protected String requestQueueName;
        protected String replyQueueName;
        protected Destination requestQueue;
        protected Destination replyQueue;

        public B threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return (B) this;
        }

        public B requestQueue(Destination requestQueue) {
            this.requestQueue = requestQueue;
            return (B) this;
        }

        public B replyQueue(Destination replyQueue) {
            this.replyQueue = replyQueue;
            return (B) this;
        }

        public B requestQueueName(String requestQueueName) {
            this.requestQueueName = requestQueueName;
            return (B) this;
        }

        public B replyQueueName(String replyQueueName) {
            this.replyQueueName = replyQueueName;
            return (B) this;
        }

        @Override
        public EzyActiveRpcEndpoint build() {
            if (requestQueue == null) {
                requestQueue = createDestination(EzyActiveDestinationType.QUEUE, requestQueueName);
            }
            if (replyQueue == null) {
                replyQueue = createDestination(EzyActiveDestinationType.QUEUE, replyQueueName);
            }
            try {
                return newProduct();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        protected abstract EzyActiveRpcEndpoint newProduct() throws Exception;
    }
}
