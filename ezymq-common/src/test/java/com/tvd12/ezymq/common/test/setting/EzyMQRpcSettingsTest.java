package com.tvd12.ezymq.common.test.setting;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.common.test.handler.EzyTestMQRequestHandler;
import com.tvd12.ezymq.common.test.handler.EzyTestMQRequestInterceptor;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;

@SuppressWarnings("rawtypes")
public class EzyMQRpcSettingsTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyTestMQRequestInterceptor interceptor1 = mock(EzyTestMQRequestInterceptor.class);
        EzyTestMQRequestInterceptor interceptor2 = mock(EzyTestMQRequestInterceptor.class);

        // when
        InternalSettings.Builder builder = new InternalSettings.Builder(null)
            .addRequestInterceptor(interceptor1)
            .addRequestInterceptor(interceptor2);

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(builder, "requestInterceptors"),
            Arrays.asList(interceptor1, interceptor2),
            false
        );
    }

    public static class InternalSettings extends EzyMQRpcSettings {

        public InternalSettings(
            Properties properties,
            Map<String, Class> requestTypeByCommand,
            Map<String, Map<String, Class>> messageTypeMapByTopic
        ) {
            super(properties, requestTypeByCommand, messageTypeMapByTopic);
        }

        public static class Builder extends EzyMQRpcSettings.Builder<
            InternalSettings,
            EzyTestMQRequestInterceptor,
            EzyTestMQRequestHandler,
            Builder> {

            public Builder(EzyMQRpcProxyBuilder parent) {
                super(parent);
            }

            @Override
            protected String getRequestCommand(Object handler) {
                return "test";
            }

            @Override
            protected EzyConsumerAnnotationProperties getConsumerAnnotationProperties(
                EzyMQMessageConsumer messageConsumer
            ) {
                return new EzyConsumerAnnotationProperties("test", "test");
            }

            @Override
            public InternalSettings build() {
                return new InternalSettings(
                    properties,
                    requestTypeByCommand,
                    messageTypeMapByTopic
                );
            }
        }
    }
}
