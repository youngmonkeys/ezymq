package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import lombok.Getter;

import javax.jms.Session;

@Getter
public class EzyActiveEndpointSetting {

    protected final Session session;

    public EzyActiveEndpointSetting(Session session) {
        this.session = session;
    }

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyActiveEndpointSetting> {

        protected Session session;

        public B session(Session session) {
            this.session = session;
            return (B) this;
        }
    }
}
