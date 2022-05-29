package com.tvd12.ezymq.kafka.endpoint;

import com.tvd12.ezyfox.concurrent.EzyExecutors;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

@SuppressWarnings("rawtypes")
public class EzyKafkaServer
    extends EzyKafkaEndpoint
    implements EzyStartable, EzyCloseable {

    protected final long pollTimeOut;
    protected final Consumer consumer;
    protected final Thread poolRecordThread;
    protected final ExecutorService executorService;
    protected volatile boolean active;
    protected final AtomicBoolean started;
    @Setter
    protected EzyKafkaRecordsHandler recordsHandler;

    public EzyKafkaServer(
        String topic,
        Consumer consumer,
        long poolTimeOut
    ) {
        this(topic, consumer, poolTimeOut, 1);
    }

    public EzyKafkaServer(
        String topic,
        Consumer consumer,
        long poolTimeOut,
        int threadPoolSize
    ) {
        this(
            topic,
            consumer,
            poolTimeOut,
            newExecutorService(topic, threadPoolSize)
        );
    }

    public EzyKafkaServer(
        String topic,
        Consumer consumer,
        long poolTimeOut,
        ExecutorService executorService
    ) {
        super(topic);
        this.consumer = consumer;
        this.pollTimeOut = poolTimeOut;
        this.started = new AtomicBoolean();
        this.executorService = executorService;
        this.poolRecordThread = newPoolRecordThread(topic);
    }

    protected static ExecutorService newExecutorService(
        String topic,
        int threadPoolSize
    ) {
        ExecutorService executorService = EzyExecutors.newFixedThreadPool(
            threadPoolSize,
            "kafka-consumer-" + topic
        );
        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown));
        return executorService;
    }

    public static Builder builder() {
        return new Builder();
    }

    protected Thread newPoolRecordThread(String topic) {
        return EzyExecutors.newThreadFactory("kafka-consumer-pool-" + topic)
            .newThread(this::loop);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            this.active = true;
            this.consumer.subscribe(Collections.singletonList(topic));
            this.poolRecordThread.start();
        } else {
            throw new IllegalStateException("server's already started");
        }
    }

    protected void loop() {
        while (active) {
            pollRecords();
        }
    }

    @SuppressWarnings("unchecked")
    protected void pollRecords() {
        try {
            ConsumerRecords records;
            synchronized (this) {
                records = consumer.poll(Duration.ofMillis(pollTimeOut));
            }
            records.forEach(record ->
                executorService.execute(() -> {
                    try {
                        recordsHandler.handleRecord((ConsumerRecord) record);
                    } catch (Throwable e) {
                        if (active) {
                            logger.warn("handle record: {} error", record, e);
                        }
                    }
                })
            );
        } catch (Exception e) {
            if (active) {
                logger.warn("poll records error", e);
            }
        }
    }

    @Override
    public void close() {
        this.active = false;
        synchronized (this) {
            processWithLogException(consumer::close);
        }
        processWithLogException(executorService::shutdown);
    }

    public static class Builder extends EzyKafkaEndpoint.Builder<Builder> {

        protected Consumer consumer;
        protected int threadPoolSize;
        protected long pollTimeOut = 100;
        protected Deserializer deserializer;
        protected EzyKafkaRecordsHandler recordsHandler;

        public Builder pollTimeOut(long pollTimeOut) {
            this.pollTimeOut = pollTimeOut;
            return this;
        }

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder consumer(Consumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder deserializer(Deserializer deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder recordsHandler(EzyKafkaRecordsHandler recordsHandler) {
            this.recordsHandler = recordsHandler;
            return this;
        }

        @Override
        public EzyKafkaServer build() {
            if (consumer == null) {
                this.consumer = newConsumer(deserializer);
            }
            return new EzyKafkaServer(topic, consumer, pollTimeOut, threadPoolSize);
        }
    }
}
