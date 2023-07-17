package com.tvd12.ezymq.mosquitto.factory;

public interface EzyMosquittoMessageIdFactory {

    int newMessageId(String topic);
}
