package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;

import javax.jms.Connection;
import javax.jms.Session;

public class ActiveMQTopicTest extends ActiveMQBaseTest {

    public static void main(String[] args) throws Exception {
        new ActiveMQTopicTest().test();
    }

    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        String topicName = "topic-test-1";
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        EzyActiveTopicServer server = EzyActiveTopicServer.builder()
            .session(session)
            .topicName(topicName)
            .build();
        EzyActiveTopicClient client = EzyActiveTopicClient.builder()
            .session(session)
            .topicName(topicName)
            .build();
        EzyActiveTopic<String> topic = EzyActiveTopic.builder()
            .client(client)
            .server(server)
            .dataCodec(dataCodec)
            .build();
        topic.addConsumer(
            "test",
            msg -> System.out.println("received: " + msg)
        );
        System.out.println("start consumer ok");
        Thread.sleep(100);
        topic.publish("test", "I'm a monkey");
        System.out.println("publish ok");
    }
}
