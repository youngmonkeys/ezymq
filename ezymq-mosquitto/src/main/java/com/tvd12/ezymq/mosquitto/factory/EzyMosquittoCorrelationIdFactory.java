package com.tvd12.ezymq.mosquitto.factory;

public interface EzyMosquittoCorrelationIdFactory {

    String newCorrelationId(String topic);
}
