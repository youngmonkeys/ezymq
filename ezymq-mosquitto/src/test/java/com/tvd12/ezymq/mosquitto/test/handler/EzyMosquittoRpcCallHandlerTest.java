package com.tvd12.ezymq.mosquitto.test.handler;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRpcCallHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class EzyMosquittoRpcCallHandlerTest {

    @Test
    public void test() {
        // give
        TestEzyMosquittoRpcCallHandler instance = new TestEzyMosquittoRpcCallHandler();
        EzyMosquittoMessage request = mock(EzyMosquittoMessage.class);
        EzyMosquittoProperties.Builder replyPropertiesBuilder = EzyMosquittoProperties.builder();

        // when
        // then
        instance.handleFire(request);
        Asserts.assertEquals(instance.handleCall(request, replyPropertiesBuilder), new byte[0]);
    }

    public static class TestEzyMosquittoRpcCallHandler implements EzyMosquittoRpcCallHandler {

        @Override
        public void handleFire(EzyMosquittoProperties requestProperties, byte[] requestBody) {
        }

        @Override
        public byte[] handleCall(EzyMosquittoProperties requestProperties, byte[] requestBody, EzyMosquittoProperties.Builder replyPropertiesBuilder) {
            return new byte[0];
        }
    }
}
