package com.tvd12.ezymq.activemq.exception;

public class EzyActiveMaxCapacity extends RuntimeException {
    private static final long serialVersionUID = -1027461702596959883L;

    public EzyActiveMaxCapacity(String msg) {
        super(msg);
    }
}
