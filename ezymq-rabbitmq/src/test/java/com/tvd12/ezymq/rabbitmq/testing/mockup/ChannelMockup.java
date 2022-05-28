package com.tvd12.ezymq.rabbitmq.testing.mockup;

import com.rabbitmq.client.AMQP.Basic.RecoverOk;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Exchange.BindOk;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Exchange.UnbindOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Tx.CommitOk;
import com.rabbitmq.client.AMQP.Tx.RollbackOk;
import com.rabbitmq.client.AMQP.Tx.SelectOk;
import com.rabbitmq.client.*;
import com.tvd12.ezyfox.util.EzyLoggable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ChannelMockup extends EzyLoggable implements Channel {

    protected static final Map<String, String> queueNames;
    protected static final Map<String, BlockingQueue> queues;
    protected static final Map<String, Consumer> consumers;

    static {
        queues = new ConcurrentHashMap<>();
        queueNames = new ConcurrentHashMap<>();
        consumers = new ConcurrentHashMap<>();
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public ShutdownSignalException getCloseReason() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void notifyListeners() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isOpen() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getChannelNumber() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Connection getConnection() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close(int closeCode, String closeMessage) {
        // TODO Auto-generated method stub

    }

    @Override
    public void abort() {
        // TODO Auto-generated method stub

    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clearReturnListeners() {
        // TODO Auto-generated method stub

    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clearConfirmListeners() {
        // TODO Auto-generated method stub

    }

    @Override
    public Consumer getDefaultConsumer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicQos(int prefetchCount, boolean global) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicQos(int prefetchCount) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) {
        String key = exchange + routingKey;
        String queueName = queueNames.get(key);
        System.out.println("publish to queue: " + queueName + ", exchange: " + exchange + ", routingKey: " + routingKey);
        if (queueName == null) {
            return;
        }
        BlockingQueue queue = queues.computeIfAbsent(queueName, k -> new LinkedBlockingQueue<>());
        //noinspection ResultOfMethodCallIgnored
        queue.offer(new Object[]{props, body});
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties props, byte[] body) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate,
                             BasicProperties props, byte[] body) {
        // TODO Auto-generated method stub

    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete,
                                     Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
                                     Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete,
                                     boolean internal, Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
                                     boolean internal, Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete,
                                      boolean internal, Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
                                      boolean internal, Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public DeclareOk exchangeDeclarePassive(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteOk exchangeDelete(String exchange, boolean ifUnused) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) {
        // TODO Auto-generated method stub

    }

    @Override
    public DeleteOk exchangeDelete(String exchange) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BindOk exchangeBind(String destination, String source, String routingKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public UnbindOk exchangeUnbind(String destination, String source, String routingKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey,
                                     Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive,
                                                                 boolean autoDelete, Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete,
                                   Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclarePassive(String queue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) {
        // TODO Auto-generated method stub

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) {
        return queueBind(queue, exchange, routingKey, null);
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(
        String queue,
        String exchange,
        String routingKey,
        Map<String, Object> arguments
    ) {
        String key = exchange + routingKey;
        queueNames.put(key, queue);
        System.out.println("bind queue: " + queue + ", exchange: " + exchange + ", routingKey: " + routingKey + ", queueSize: " + queueNames.size());
        return new com.rabbitmq.client.AMQP.Queue.BindOk.Builder().build();
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        // TODO Auto-generated method stub

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey,
                                                               Map<String, Object> arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PurgeOk queuePurge(String queue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        // TODO Auto-generated method stub

    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {
        // TODO Auto-generated method stub

    }

    @Override
    public String basicConsume(String queue, Consumer callback) {
        return basicConsume(queue, true, callback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, true, new Consumer() {

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
            }

            @Override
            public void handleDelivery(String arg0, Envelope arg1, BasicProperties arg2, byte[] arg3) throws IOException {
                Delivery delivery = new Delivery(arg1, arg2, arg3);
                deliverCallback.handle(arg0, delivery);
            }

            @Override
            public void handleConsumeOk(String consumerTag) {
            }

            @Override
            public void handleCancelOk(String consumerTag) {
            }

            @Override
            public void handleCancel(String consumerTag) {
            }
        });
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) {
        consumers.put(queue, callback);
        BlockingQueue q = queues.computeIfAbsent(queue, k -> new LinkedBlockingQueue<>());
        Thread thread = new Thread(() -> {
            int i = 0;
            while ((i++) < 1000) {
                try {
                    Object[] m = (Object[]) q.take();
                    System.out.println("consume queue: " + queue);
                    Envelope envelope = new Envelope(1, true, "", "");
                    callback.handleDelivery("", envelope, (BasicProperties) m[0], (byte[]) m[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.setName("mockup-basic-consume");
        thread.start();
        return "OK";
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
                               CancelCallback cancelCallback) {
        return basicConsume(queue, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
                               CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                               DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                               DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                               DeliverCallback deliverCallback, CancelCallback cancelCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
                               CancelCallback cancelCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
                               CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive,
                               Map<String, Object> arguments, Consumer callback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive,
                               Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive,
                               Map<String, Object> arguments, DeliverCallback deliverCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive,
                               Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback,
                               ConsumerShutdownSignalCallback shutdownSignalCallback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
        String action = "cancel";
        String queueName = consumerTag;
        if (consumerTag.startsWith("cancel_")) {
            queueName = consumerTag.substring("cancel_".length());
        } else if (consumerTag.startsWith("shutdown_")) {
            action = "shutdown";
            queueName = consumerTag.substring("shutdown_".length());
        }
        Consumer consumer = consumers.get(queueName);
        System.out.println("basic cancel, action: " + action + ", queue: " + queueName + ", consumer: " + consumer);
        if (consumer == null) {
            return;
        }
        if ("shutdown".equals(action)) {
            consumer.handleShutdownSignal(consumerTag, new ShutdownSignalException(true, true, null, null));
        } else {
            consumer.handleCancel(queueName);
        }
    }

    @Override
    public RecoverOk basicRecover() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RecoverOk basicRecover(boolean requeue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SelectOk txSelect() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CommitOk txCommit() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RollbackOk txRollback() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public com.rabbitmq.client.AMQP.Confirm.SelectOk confirmSelect() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getNextPublishSeqNo() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean waitForConfirms() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean waitForConfirms(long timeout) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void waitForConfirmsOrDie() {
        // TODO Auto-generated method stub

    }

    @Override
    public void waitForConfirmsOrDie(long timeout) {
        // TODO Auto-generated method stub

    }

    @Override
    public void asyncRpc(Method method) {
        // TODO Auto-generated method stub

    }

    @Override
    public Command rpc(Method method) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long messageCount(String queue) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long consumerCount(String queue) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) {
        // TODO Auto-generated method stub
        return null;
    }

}
