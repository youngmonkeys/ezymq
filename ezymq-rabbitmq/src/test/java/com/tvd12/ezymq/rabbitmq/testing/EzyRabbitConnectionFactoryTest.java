package com.tvd12.ezymq.rabbitmq.testing;

import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;

public class EzyRabbitConnectionFactoryTest {

    @Test
    public void test() {
        EzyRabbitConnectionFactory factory = new EzyRabbitConnectionFactory();
        factory.setSharedExecutor(Executors.newSingleThreadExecutor());
        try {
            factory.newConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            factory.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
