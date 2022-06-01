package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.ezymq.activemq.test.response.SumResponse;

public class SumRpcClientProgram {

    public static void main(String[] args) {
        EzyActiveMQProxy proxy = EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test")
            .build();
        EzyActiveRpcProducer producer = proxy.getRpcProducer("test");
        SumResponse sumResponse = producer.call(
            "sum",
            new SumRequest(1, 2),
            SumResponse.class
        );
        System.out.println("sum result: " + sumResponse);
    }
}
