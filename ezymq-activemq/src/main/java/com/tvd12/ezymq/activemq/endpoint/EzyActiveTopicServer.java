package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.activemq.concurrent.EzyActiveThreadFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import lombok.Setter;

import javax.jms.*;
import java.util.concurrent.ThreadFactory;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyActiveTopicServer
    extends EzyActiveTopicEndpoint
    implements EzyStartable {

    protected volatile boolean active;
    protected final int threadPoolSize;
    protected final MessageConsumer consumer;
    protected final EzyThreadList executorService;
    @Setter
    protected EzyActiveMessageHandler messageHandler;

    public EzyActiveTopicServer(
        Session session,
        Destination topic,
        int threadPoolSize
    ) throws Exception {
        super(session, topic);
        this.threadPoolSize = threadPoolSize;
        this.executorService = newExecutorService();
        this.consumer = session.createConsumer(topic);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() throws Exception {
        this.active = true;
        this.executorService.execute();
    }

    protected void loop() {
        while (active) {
            BytesMessage message = null;
            try {
                message = (BytesMessage) consumer.receive();
                if (message == null) {
                    return;
                }
                EzyActiveProperties props = getMessageProperties(message);
                byte[] body = getMessageBody(message);
                messageHandler.handle(props, body);
            } catch (JMSException e) {
                logger.warn("receive topic message error", e);
            } catch (Throwable e) {
                logger.warn("process message: {} error", message, e);
            }
        }
    }

    protected EzyThreadList newExecutorService() {
        ThreadFactory threadFactory
            = EzyActiveThreadFactory.create("topic-server");
        return new EzyThreadList(threadPoolSize, this::loop, threadFactory);
    }

    @Override
    public void close() {
        this.active = false;
        processWithLogException(executorService::interrupt);
        processWithLogException(consumer::close);
    }

    public static class Builder extends EzyActiveTopicEndpoint.Builder<Builder> {

        protected int threadPoolSize = 1;

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        @Override
        public EzyActiveTopicServer build() {
            return (EzyActiveTopicServer) super.build();
        }

        @Override
        protected EzyActiveTopicEndpoint newEndpoint() throws Exception {
            return new EzyActiveTopicServer(session, topic, threadPoolSize);
        }
    }
}
