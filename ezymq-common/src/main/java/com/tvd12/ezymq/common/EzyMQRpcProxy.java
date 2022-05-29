package com.tvd12.ezymq.common;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.setting.EzyMQSettings;

public abstract class EzyMQRpcProxy<S extends EzyMQSettings>
    extends EzyMQProxy<S, EzyMQDataCodec> {

    public EzyMQRpcProxy(
        S settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec
    ) {
        super(settings, dataCodec, entityCodec);
    }
}
