package com.tvd12.ezymq.kafka.testing;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezyfox.identifier.EzyIdFetchers;
import com.tvd12.ezyfox.identifier.EzySimpleIdFetcherImplementer;
import com.tvd12.ezyfox.message.EzyMessageIdFetchers;
import com.tvd12.ezymq.kafka.EzyKafkaConsumer;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesEntityCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.testing.entity.KafkaChatMessage;
import com.tvd12.test.base.BaseTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;

@SuppressWarnings("rawtypes")
public class EzyKafkaClientServerTest extends BaseTest {

    protected final static String TOPIC = "my-example-topic";
    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;

    public EzyKafkaClientServerTest() {
        EzyBindingContext bindingContext = newBindingContext();
        marshaller = bindingContext.newMarshaller();
        unmarshaller = bindingContext.newUnmarshaller();
        messageSerializer = newMessageSerializer();
        messageDeserializer = newMessageDeserializer();
    }

    public static void main(String[] args) throws Exception {
        new EzyKafkaClientServerTest().run();
    }

    private void run() throws Exception {
        EzyKafkaProducer client = newClient();
        runClient(client, 5);
        runServer();
        Thread.sleep(3000L);
    }

    private EzyKafkaConsumer newServer() {
        Consumer consumer = newConsumer();
        EzyKafkaServer server = new EzyKafkaServer(TOPIC, consumer, 100);
        EzyKafkaDataCodec dataCodec = EzyKafkaBytesDataCodec.builder()
            .unmarshaller(unmarshaller)
            .messageDeserializer(messageDeserializer)
            .mapMessageType(TOPIC, KafkaChatMessage.class)
            .build();
        EzyKafkaMessageHandlers requestHandlers = new EzyKafkaMessageHandlers();
        requestHandlers.addHandler(TOPIC, new EzyKafkaMessageHandler<KafkaChatMessage>() {
            @Override
            public void process(KafkaChatMessage message) {
                System.out.println("GREAT! We have just received message: " + message);
            }
        });
        return new EzyKafkaConsumer(server, dataCodec, requestHandlers);
    }

    private EzyKafkaProducer newClient() {
        EzySimpleIdFetcherImplementer.setDebug(true);
        Producer producer = newProducer();
        EzyKafkaClient client = new EzyKafkaClient(null, producer);
        EzyEntityCodec entityCodec = EzyKafkaBytesEntityCodec.builder()
            .marshaller(marshaller)
            .messageSerializer(messageSerializer)
            .build();
        return new EzyKafkaProducer(client, entityCodec);
    }

    @SuppressWarnings("unchecked")
    private Consumer newConsumer() {
        Consumer consumer = TestUtil.newConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private Producer newProducer() {
        return TestUtil.newProducer();
    }

    private EzyBindingContext newBindingContext() {
        return EzyBindingContext.builder()
            .scan("com.tvd12.ezymq.kafka.testing.entity")
            .build();
    }

    protected EzyIdFetchers newMessageIdFetchers() {
        return EzyMessageIdFetchers.builder()
            .scan("com.tvd12.ezymq.kafka.testing.entity")
            .build();
    }

    protected EzyMessageSerializer newMessageSerializer() {
        return new MsgPackSimpleSerializer();
    }

    protected EzyMessageDeserializer newMessageDeserializer() {
        return new MsgPackSimpleDeserializer();
    }

    private void runServer() {
        EzyKafkaConsumer server = newServer();
        server.start();
    }

    private void runClient(EzyKafkaProducer client, int sendMessageCount) throws Exception {
        long time = System.currentTimeMillis();
        for (long index = time; index < time + sendMessageCount; index++) {
            KafkaChatMessage message = new KafkaChatMessage(index, "Meessage#" + index);
            client.send(TOPIC, message);
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(value=%s), time=%d\n", message, elapsedTime);
        }
    }
}
