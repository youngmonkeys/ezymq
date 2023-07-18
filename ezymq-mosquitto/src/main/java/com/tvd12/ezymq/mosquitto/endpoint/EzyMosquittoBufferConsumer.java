package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class EzyMosquittoBufferConsumer implements EzyCloseable {

    protected volatile Exception exception;
    protected final BlockingQueue<EzyMosquittoMessage> queue;

    protected static final EzyMosquittoMessage POISON =
        new EzyMosquittoMessage(null, null);

    public EzyMosquittoBufferConsumer() {
        this.queue = new LinkedBlockingQueue<>();
    }

    public EzyMosquittoMessage nextDelivery() throws Exception {
        EzyMosquittoMessage delivery = queue.take();
        if (delivery == POISON) {
            if (exception != null) {
                throw exception;
            }
            return null;
        }
        return delivery;
    }

    public void handleShutdownSignal(
        EzyMqttConnectionLostException sig
    ) {
        this.exception = sig;
        this.queue.add(POISON);
    }

    public void handleDelivery(EzyMosquittoMessage message) {
        this.queue.add(message);
    }

    @Override
    public void close() {
        this.queue.add(POISON);
    }
}