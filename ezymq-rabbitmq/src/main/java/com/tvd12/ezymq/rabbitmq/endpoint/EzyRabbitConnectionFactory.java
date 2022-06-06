package com.tvd12.ezymq.rabbitmq.endpoint;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyThreads;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyRabbitConnectionFactory
    extends ConnectionFactory
    implements EzyCloseable {

    @Setter
    protected int maxConnectionAttempts;
    protected ExecutorService copyExecutorService;
    protected final List<Connection> createdConnections =
        Collections.synchronizedList(new ArrayList<>());
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void setSharedExecutor(ExecutorService executor) {
        super.setSharedExecutor(executor);
        this.copyExecutorService = executor;
    }

    @Override
    public Connection newConnection(
        ExecutorService executor,
        AddressResolver addressResolver,
        String clientProvidedName
    ) throws IOException, TimeoutException {
        int retryCount = 0;
        Connection connection;
        while (true) {
            try {
                connection = super.newConnection(
                    executor,
                    addressResolver,
                    clientProvidedName
                );
                break;
            } catch (Throwable e) {
                if (retryCount >= maxConnectionAttempts) {
                    throw e;
                }
                logger.error(
                    "can not get redis client, retry count: {}",
                    ++retryCount,
                    e
                );
                EzyThreads.sleep(3000);
            }
        }
        createdConnections.add(connection);
        return connection;
    }

    @Override
    public void close() {
        for (Connection connection : createdConnections) {
            processWithLogException(connection::close);
        }
        if (copyExecutorService != null) {
            processWithLogException(copyExecutorService::shutdown);
        }
    }
}
