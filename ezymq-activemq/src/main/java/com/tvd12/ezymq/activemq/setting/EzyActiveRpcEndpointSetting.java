package com.tvd12.ezymq.activemq.setting;

import lombok.Getter;

import javax.jms.Destination;
import javax.jms.Session;

import static com.tvd12.ezyfox.io.EzyStrings.isEmpty;

@Getter
public class EzyActiveRpcEndpointSetting extends EzyActiveEndpointSetting {

    protected int threadPoolSize;
    protected String requestQueueName;
    protected String replyQueueName;
    protected Destination requestQueue;
    protected Destination replyQueue;

    public EzyActiveRpcEndpointSetting(
        Session session,
        String requestQueueName,
        Destination requestQueue,
        String replyQueueName,
        Destination replyQueue,
        int threadPoolSize
    ) {
        super(session);
        this.threadPoolSize = threadPoolSize;
        this.requestQueue = requestQueue;
        this.replyQueue = replyQueue;
        this.requestQueueName = requestQueueName;
        this.replyQueueName = replyQueueName;
    }

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        extends EzyActiveEndpointSetting.Builder<B> {

        protected int threadPoolSize = 1;
        protected String requestQueueName;
        protected String replyQueueName;
        protected Destination requestQueue;
        protected Destination replyQueue;

        public B threadPoolSize(int threadPoolSize) {
            if (threadPoolSize > 0) {
                this.threadPoolSize = threadPoolSize;
            }
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
            if (isEmpty(this.replyQueueName)) {
                this.requestQueueName = requestQueueName;
            }
            return (B) this;
        }

        public B replyQueueName(String replyQueueName) {
            if (isEmpty(this.replyQueueName)) {
                this.replyQueueName = replyQueueName;
            }
            return (B) this;
        }
    }
}
