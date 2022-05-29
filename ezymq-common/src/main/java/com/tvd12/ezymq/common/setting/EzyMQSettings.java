package com.tvd12.ezymq.common.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyPropertiesKeeper;
import com.tvd12.ezymq.common.EzyMQProxyBuilder;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
@SuppressWarnings("rawtypes")
public class EzyMQSettings {

    public List<Class> getMessageTypeList() {
        return Collections.emptyList();
    }

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
