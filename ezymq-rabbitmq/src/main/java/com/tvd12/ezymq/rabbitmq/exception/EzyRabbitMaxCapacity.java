package com.tvd12.ezymq.rabbitmq.exception;

public class EzyRabbitMaxCapacity extends RuntimeException {
    private static final long serialVersionUID = -1027461702596959883L;

    public EzyRabbitMaxCapacity(String msg) {
        super(msg);
    }
}
