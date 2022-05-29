package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.impl.DefaultExceptionHandler;
import com.tvd12.ezymq.rabbitmq.concurrent.EzyRabbitThreadFactory;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyRabbitConnectionFactoryBuilderTest extends BaseTest {

    @Test
    public void test() {
        new EzyRabbitConnectionFactoryBuilder()
            .uri("amqp://guest:guest@127.0.0.1:5672")
            .host("127.0.0.1")
            .port(5672)
            .vhost("/")
            .username("guest")
            .password("guest")
            .requestedHeartbeat(0)
            .threadFactory(EzyRabbitThreadFactory.create("test"))
            .exceptionHandler(new DefaultExceptionHandler())
            .build();

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test2() {
        new EzyRabbitConnectionFactoryBuilder()
            .threadFactory("test")
            .uri("a//guest:guest@127.0.0.1:5672")
            .build();
    }

}
