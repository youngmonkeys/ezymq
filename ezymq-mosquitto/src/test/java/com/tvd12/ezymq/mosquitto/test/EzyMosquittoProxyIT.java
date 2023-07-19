package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezymq.mosquitto.EzyMosquittoProxy;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxyBuilder;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

public class EzyMosquittoProxyIT {

    public static void main(String[] args) {
        String topicName = "mytopic";
        EzyMosquittoSettings settings = EzyMosquittoSettings.builder()
            .topicSettingBuilder(topicName)
            .consumerEnable(true)
            .producerEnable(true)
            .parent()
            .mapTopicMessageType(
                topicName,
                "",
                String.class
            )
            .mapRequestType("hello", String.class)
            .rpcProducerSettingBuilder("hello")
            .topic("rpc")
            .replyTopic("reply")
            .parent()
            .rpcConsumerSettingBuilder("hello")
            .topic("rpc")
            .replyTopic("reply")
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
            .build();
        EzyMosquittoProxy mosquittoProxy = new EzyMosquittoProxyBuilder()
            .settings(settings)
            .build();
        EzyMosquittoTopic<String> topic = mosquittoProxy.getTopic(topicName);
        topic.addConsumer((message) -> {
            System.out.println("topic received: " + message);
        });
        topic.publish("", "Hello World");

        EzyMosquittoRpcProducer producer = mosquittoProxy.getRpcProducer("hello");
        String answer = producer.call(
            "hello",
            "Young Monkeys",
            String.class
        );
        System.out.println("rpc answer: " + answer);
    }
}
