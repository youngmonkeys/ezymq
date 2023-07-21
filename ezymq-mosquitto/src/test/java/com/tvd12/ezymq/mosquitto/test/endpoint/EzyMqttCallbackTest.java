package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import org.testng.annotations.Test;

public class EzyMqttCallbackTest {

    @Test
    public void connectionLostTest() {
        ((EzyMqttCallback) (properties, body) -> {}).connectionLost(
            new EzyMqttConnectionLostException(
                new RuntimeException("test")
            )
        );
    }
}
