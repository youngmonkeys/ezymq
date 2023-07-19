package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezymq.mosquitto.EzyMosquittoProxy;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxyBuilder;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;
import com.tvd12.properties.file.reader.BaseFileReader;

public class EzyMosquittoProxyWithConfigFileIT {

    public static void main(String[] args) {
        EzyMosquittoSettings settings = EzyMosquittoSettings.builder()
            .mapTopicMessageType(
                "hello",
                "",
                String.class
            )
            .mapRequestType("hello", String.class)
            .rpcProducerSettingBuilder("test")
            .replyTopic("test-reply")
            .parent()
            .rpcConsumerSettingBuilder("test")
            .replyTopic("test-reply")
            .addRequestHandler("hello", new EzyMosquittoRequestHandler<String>() {
                @Override
                public Object handle(String request) {
                    return "Hello " + request + "!";
                }

                @Override
                public Class<?> getRequestType() {
                    return String.class;
                }
            })
            .parent()
            .properties(new BaseFileReader().read("application-it.yaml"))
            .build();
        EzyMosquittoProxy mosquittoProxy = new EzyMosquittoProxyBuilder()
            .settings(settings)
            .build();
        EzyMosquittoTopic<String> topic = mosquittoProxy.getTopic("hello");
        topic.addConsumer((message) -> {
            System.out.println("topic received: " + message);
        });
        topic.publish("", "Hello World");

        EzyMosquittoRpcProducer producer = mosquittoProxy.getRpcProducer("test");
        String answer = producer.call(
            "hello",
            "Young Monkeys",
            String.class
        );
        System.out.println("rpc answer: " + answer);
    }
}
