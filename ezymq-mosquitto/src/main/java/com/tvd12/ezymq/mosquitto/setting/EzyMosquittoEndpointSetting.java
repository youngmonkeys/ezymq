package com.tvd12.ezymq.mosquitto.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;

import lombok.Getter;

@Getter
public class EzyMosquittoEndpointSetting {

    protected final String topic;

    public EzyMosquittoEndpointSetting(String topic) {
        this.topic = topic;
    }

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyMosquittoEndpointSetting> {

        protected String topic;

        public B topic(String topic) {
            this.topic = topic;
            return (B) this;
        }
    }
}
