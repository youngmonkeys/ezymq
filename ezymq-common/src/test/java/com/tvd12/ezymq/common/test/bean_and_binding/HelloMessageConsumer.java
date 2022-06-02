package com.tvd12.ezymq.common.test.bean_and_binding;

import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.test.annotation.EzyTestConsumer;

@EzyTestConsumer(
    topic = "hello",
    command = "test"
)
public class HelloMessageConsumer
        implements EzyMQMessageConsumer<HelloMessage> {
    @Override
    public void consume(HelloMessage message) {}

    @Override
    public Class<?> getMessageType() {
        return HelloMessage.class;
    }
}
