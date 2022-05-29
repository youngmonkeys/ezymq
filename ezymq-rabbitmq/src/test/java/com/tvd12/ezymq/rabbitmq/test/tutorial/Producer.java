package com.tvd12.ezymq.rabbitmq.test.tutorial;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeoutException;


/**
 * The producer endpoint that writes to the queue.
 */
public class Producer extends EndPoint {

    public Producer(String endPointName) throws IOException, TimeoutException {
        super(endPointName);
    }

    public void sendMessage(Serializable object) throws IOException {
        channel.basicPublish("", endPointName, null, new byte[0]);
    }
}
