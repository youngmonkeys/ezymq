package com.tvd12.ezymq.rabbitmq.test.handler;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyRabbitMessageHandlerTest {

    @Test
    public void handleRequestTest() {
        // given
        EzyRabbitMessageHandler instance = new TestEzyRabbitMessageHandler();
        Delivery delivery = mock(Delivery.class);

        // when
        instance.handle(delivery);

        // then
        verify(delivery, times(1)).getProperties();
        verify(delivery, times(1)).getBody();
        verifyNoMoreInteractions(delivery);
    }

    public static class TestEzyRabbitMessageHandler
        implements EzyRabbitMessageHandler {

        @Override
        public void handle(
            AMQP.BasicProperties properties,
            byte[] messageBody
        ) {}
    }
}
