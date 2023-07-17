package com.tvd12.ezymq.mosquitto.endpoint;

import java.util.Properties;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

public class EzyMqttClientFactoryBuilder
        implements EzyBuilder<EzyMqttClientFactory> {

    protected String serverUri = "tcp://127.0.0.1:1883";
    protected String clientIdPrefix = "ezymq-mosquitto-";
    protected String username;
    protected String password;
    protected int maxConnectionAttempts;

    public EzyMqttClientFactoryBuilder serverUri(String serverUri) {
        this.serverUri = serverUri;
        return this;
    }

    public EzyMqttClientFactoryBuilder clientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
        return this;
    }

    public EzyMqttClientFactoryBuilder username(String username) {
        this.username = username;
        return this;
    }

    public EzyMqttClientFactoryBuilder password(String password) {
        this.password = password;
        return this;
    }

    public EzyMqttClientFactoryBuilder maxConnectionAttempts(
        int maxConnectionAttempts
    ) {
        this.maxConnectionAttempts = maxConnectionAttempts;
        return this;
    }

    public EzyMqttClientFactoryBuilder properties(Properties properties) {
        this.maxConnectionAttempts = Integer.parseInt(
            properties
                .getOrDefault(EzyMosquittoSettings.KEY_MAX_CONNECTION_ATTEMPTS, maxConnectionAttempts)
                .toString()
        );
        this.serverUri = properties.getProperty(EzyMosquittoSettings.KEY_SERVER_URI, serverUri);
        this.clientIdPrefix = properties.getProperty(EzyMosquittoSettings.KEY_CLIENT_PREFIX, clientIdPrefix);
        this.username = properties.getProperty(EzyMosquittoSettings.KEY_USERNAME, username);
        this.password = properties.getProperty(EzyMosquittoSettings.KEY_PASSWORD, password);
        return this;
    }

    @Override
    public EzyMqttClientFactory build() {
        return new EzyMqttClientFactory(
            serverUri,
            clientIdPrefix,
            username,
            password,
            maxConnectionAttempts
        );
    }
}
