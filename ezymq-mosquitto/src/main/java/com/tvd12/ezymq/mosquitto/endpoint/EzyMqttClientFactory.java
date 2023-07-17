package com.tvd12.ezymq.mosquitto.endpoint;

import static com.tvd12.ezyfox.io.EzyStrings.isNotEmpty;
import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyThreads;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EzyMqttClientFactory
    extends EzyLoggable
    implements EzyCloseable {

    protected final String serverUri;
    protected final String clientIdPrefix;
    protected final String username;
    protected final String password;
    protected final int maxConnectionAttempts;
    protected final AtomicInteger clientIdGenerator =
        new AtomicInteger();
    protected final List<MqttClient> createdMqttClients =
        Collections.synchronizedList(new ArrayList<>());

    public MqttClient newMqttClient(
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        int retryCount = 0;
        MqttClient mqttClient;
        while (true) {
            try {
                mqttClient = new MqttClient(
                    serverUri,
                    clientIdPrefix + clientIdGenerator.incrementAndGet()
                );
                MqttConnectOptions options = new MqttConnectOptions();
                if (isNotEmpty(username)) {
                    options.setUserName(username);
                }
                if (isNotEmpty(password)) {
                    options.setPassword(password.toCharArray());
                }
                mqttClient.connect();
                mqttClient.setCallback(mqttCallbackProxy);
                break;
            } catch (Throwable e) {
                if (retryCount >= maxConnectionAttempts) {
                    throw new IllegalStateException(e);
                }
                logger.error(
                    "can not connect to the broker, retry count: {}",
                    ++retryCount,
                    e
                );
                EzyThreads.sleep(3000);
            }
        }
        createdMqttClients.add(mqttClient);
        return mqttClient;
    }

    @Override
    public void close() {
        for (MqttClient mqttClient : createdMqttClients) {
            processWithLogException(mqttClient::close);
        }
    }
}
