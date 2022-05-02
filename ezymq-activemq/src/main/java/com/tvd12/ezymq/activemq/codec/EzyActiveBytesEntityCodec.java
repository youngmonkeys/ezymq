package com.tvd12.ezymq.activemq.codec;

import com.tvd12.ezyfox.binding.codec.EzyBindingEntityCodec;
import com.tvd12.ezyfox.codec.EzyEntityCodec;

public class EzyActiveBytesEntityCodec extends EzyBindingEntityCodec {

    protected EzyActiveBytesEntityCodec(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyBindingEntityCodec.Builder {

        @Override
        public EzyEntityCodec build() {
            return new EzyActiveBytesEntityCodec(this);
        }
    }
}
