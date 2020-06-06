package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.tvd12.ezyfox.util.EzyCloseable;

public class EzyRabbitBufferConsumer 
		extends DefaultConsumer implements EzyCloseable {

	protected volatile Exception exception;
    protected final BlockingQueue<Delivery> queue;
    protected final static Delivery POISON = new Delivery(null, null, null);

    public EzyRabbitBufferConsumer(Channel channel) {
    	super(channel);
        this.queue = new LinkedBlockingQueue<>();
    }

    public Delivery nextDelivery() throws Exception {
        Delivery delivery = queue.take();
        if(delivery == POISON) {
        	if(exception != null)
        		throw exception;
        	return null;
        }
        return delivery;
    }

    @Override
    public void handleShutdownSignal(
    		String consumerTag, ShutdownSignalException sig) {
    	this.exception = sig;
        this.queue.add(POISON);
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
    	this.exception = new CancellationException(
    			"consumer: " + consumerTag + " has cancelled");
        this.queue.add(POISON);
    }

    @Override
    public void handleDelivery(String consumerTag,
        Envelope envelope,
        AMQP.BasicProperties properties,
        byte[] body) throws IOException {
        this.queue.add(new Delivery(envelope, properties, body));
    }
    
    @Override
    public void close() {
    	this.queue.add(POISON);
    }

}