package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;

public class SumRpcServerProgram {

    public static void main(String[] args) {
        EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test")
            .build();
    }
}
