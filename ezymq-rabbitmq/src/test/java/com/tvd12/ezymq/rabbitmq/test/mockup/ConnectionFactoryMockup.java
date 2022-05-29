package com.tvd12.ezymq.rabbitmq.test.mockup;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConnectionFactoryMockup extends ConnectionFactory {

    @Override
    public Connection newConnection() throws IOException, TimeoutException {
        return new ConnectionMockup();
    }

}
