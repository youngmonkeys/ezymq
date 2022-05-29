package com.tvd12.ezymq.rabbitmq.endpoint;

import com.rabbitmq.client.*;
import com.tvd12.ezyfox.util.EzyCloseable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;

public class EzyRabbitBufferConsumer
    extends DefaultConsumer
    implements EzyCloseable {

    protected volatile Exception exception;
    protected final BlockingQueue<Delivery> queue;

    protected static final Delivery POISON =
        new Delivery(null, null, null);

    public EzyRabbitBufferConsumer(Channel channel) {
        super(channel);
        this.queue = new LinkedBlockingQueue<>();
    }

    public Delivery nextDelivery() throws Exception {
        Delivery delivery = queue.take();
        if (delivery == POISON) {
            if (exception != null) {
                throw exception;
            }
            return null;
        }
        return delivery;
    }

    @Override
    public void handleShutdownSignal(
        String consumerTag,
        ShutdownSignalException sig
    ) {
        this.exception = sig;
        this.queue.add(POISON);
    }

    @Override
    public void handleCancel(String consumerTag) {
        this.exception = new CancellationException(
            "consumer: " + consumerTag + " has cancelled"
        );
        this.queue.add(POISON);
    }

    @Override
    public void handleDelivery(
        String consumerTag,
        Envelope envelope,
        AMQP.BasicProperties properties,
        byte[] body
    ) {
        this.queue.add(new Delivery(envelope, properties, body));
    }

    @Override
    public void close() {
        this.queue.add(POISON);
    }
}