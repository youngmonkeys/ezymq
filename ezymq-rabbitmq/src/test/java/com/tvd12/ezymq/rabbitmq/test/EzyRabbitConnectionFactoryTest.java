package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;

public class EzyRabbitConnectionFactoryTest extends BaseTest {

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
