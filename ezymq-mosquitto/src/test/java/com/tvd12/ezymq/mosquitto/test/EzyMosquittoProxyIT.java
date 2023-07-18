package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezymq.mosquitto.EzyMosquittoProxy;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxyBuilder;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
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
            .build();
        EzyMosquittoProxy mosquittoProxy = new EzyMosquittoProxyBuilder()
            .settings(settings)
            .build();
        EzyMosquittoTopic<String> topic = mosquittoProxy.getTopic(topicName);
        topic.addConsumer((message) -> {
            System.out.println(message);
        });
        topic.publish("", "Hello World");
    }
}
