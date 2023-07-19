package com.tvd12.ezymq.mosquitto.exception;

public class EzyMosquittoMaxCapacity extends RuntimeException {
    private static final long serialVersionUID = -1027461702596959883L;

    public EzyMosquittoMaxCapacity(String msg) {
        super(msg);
    }
}
