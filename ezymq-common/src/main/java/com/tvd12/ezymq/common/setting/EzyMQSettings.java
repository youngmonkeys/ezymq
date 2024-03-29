package com.tvd12.ezymq.common.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyPropertiesKeeper;
import com.tvd12.ezymq.common.EzyMQProxyBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Properties;
import java.util.Set;

@Getter
@AllArgsConstructor
@SuppressWarnings("rawtypes")
public abstract class EzyMQSettings {

    protected final Properties properties;

    public abstract Set<Class> getMessageTypes();

    public abstract static class Builder<
        S extends EzyMQSettings,
        B extends Builder<S, B>
        >
        extends EzyPropertiesKeeper<B>
        implements EzyBuilder<S> {

        protected final EzyMQProxyBuilder parent;

        public Builder(EzyMQProxyBuilder parent) {
            this.parent = parent;
        }

        public EzyMQProxyBuilder parent() {
            return parent;
        }
    }
}
