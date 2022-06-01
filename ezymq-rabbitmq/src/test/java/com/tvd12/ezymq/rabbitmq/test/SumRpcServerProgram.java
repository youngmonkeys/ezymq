package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;

public class SumRpcServerProgram {

    public static void main(String[] args) {
        EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test")
            .build();
    }
}
