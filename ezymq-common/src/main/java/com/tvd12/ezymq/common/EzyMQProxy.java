package com.tvd12.ezymq.common;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.common.setting.EzyMQSettings;

public abstract class EzyMQProxy<S extends EzyMQSettings, D>
    implements EzyCloseable {

    protected final S settings;
    protected final EzyEntityCodec entityCodec;
    protected final D dataCodec;

    public EzyMQProxy(
        S settings,
        D dataCodec,
        EzyEntityCodec entityCodec
    ) {
        this.settings = settings;
        this.dataCodec = dataCodec;
        this.entityCodec = entityCodec;
    }
}
