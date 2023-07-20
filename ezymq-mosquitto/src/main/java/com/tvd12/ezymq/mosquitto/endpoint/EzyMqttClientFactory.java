package com.tvd12.ezymq.mosquitto.endpoint;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import lombok.AllArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tvd12.ezyfox.io.EzyStrings.isNotEmpty;
import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

@AllArgsConstructor
public class EzyMqttClientFactory
    extends EzyLoggable
    implements EzyCloseable {

    protected final String serverUri;
    protected final String clientIdPrefix;
    protected final String username;
    protected final String password;
    protected final int maxConnectionAttempts;
    protected final int connectionAttemptSleepTime;
    protected final EzyMqttMqMessageCodec mqttMqMessageCodec;
    protected final AtomicInteger clientIdGenerator =
        new AtomicInteger();
    protected final List<EzyMqttClientProxy> createdMqttClients =
        Collections.synchronizedList(new ArrayList<>());

    public EzyMqttClientProxy newMqttClient() {
        int retryCount = 0;
        EzyMqttClientProxy mqttClientProxy;
        while (true) {
            try {
                MqttClient mqttClient = new MqttClient(
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
                mqttClientProxy = new EzyMqttClientProxy(
                    mqttClient,
                    mqttMqMessageCodec
                );
                mqttClientProxy.connect();
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
                EzyThreads.sleep(connectionAttemptSleepTime);
            }
        }
        createdMqttClients.add(mqttClientProxy);
        return mqttClientProxy;
    }

    @Override
    public void close() {
        for (EzyMqttClientProxy mqttClient : createdMqttClients) {
            processWithLogException(mqttClient::close);
        }
    }
}
