package com.tvd12.ezymq.common.test;

import com.tvd12.ezyfox.bean.EzyBeanContext;
import com.tvd12.ezyfox.bean.EzyBeanContextBuilder;
import com.tvd12.ezyfox.bean.EzySingletonFactory;
import com.tvd12.ezyfox.bean.impl.EzyBeanKey;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.EzyMQProxy;
import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.common.test.annotation.EzyTestConsumer;
import com.tvd12.ezymq.common.test.annotation.EzyTestHandler;
import com.tvd12.ezymq.common.test.annotation.EzyTestInterceptor;
import com.tvd12.ezymq.common.test.codec.EzyMQBytesDataCodecTest;
import com.tvd12.ezymq.common.test.handler.EzyTestMQRequestHandler;
import com.tvd12.ezymq.common.test.handler.EzyTestMQRequestInterceptor;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.*;

@SuppressWarnings("rawtypes")
public class EzyMQRpcProxyBuilderTest extends BaseTest {

    @Test
    public void test() {
        // given
        String command1 = RandomUtil.randomShortAlphabetString();
        String command2 = RandomUtil.randomShortAlphabetString();
        String command3 = RandomUtil.randomShortAlphabetString();
        String topic1 = RandomUtil.randomShortAlphabetString();
        String topic2 = RandomUtil.randomShortAlphabetString();
        String topic3 = RandomUtil.randomShortAlphabetString();
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        Object singleton1 = new Singleton1();
        Object singleton2 = new Singleton2();
        Object singleton3 = new Singleton3();
        Object singleton4 = new Singleton4();
        Object singleton5 = new Singleton5();
        Object singleton6 = new Singleton6();

        EzyBeanContext beanContext = mock(EzyBeanContext.class);
        when(
            beanContext.getSingletonMapByKey()
        ).thenReturn(
            singletonMap(
                EzyBeanKey.of(
                    "singleton6",
                    Object.class
                ),
                singleton6
            )
        );
        EzyBindingContext bindingContext = mock(EzyBindingContext.class);
        boolean scanAndAddAllBeans = RandomUtil.randomBoolean();
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        EzyMessageDeserializer textMessageDeserializer = mock(EzyMessageDeserializer.class);

        InternalMQRpcSettings settings = new InternalMQRpcSettings.Builder(null)
            .build();

        // when
        InternalMQRpcProxyBuilder sut = new InternalMQRpcProxyBuilder()
            .scan("com.tvd12.ezymq.common.test1")
            .scan(
                "com.tvd12.ezymq.common.test2",
                "com.tvd12.ezymq.common.test3"
            )
            .scan(
                Arrays.asList(
                    "com.tvd12.ezymq.common.test4",
                    "com.tvd12.ezymq.common.test5"
                )
            )
            .entryClass(
                EzyMQBytesDataCodecTest.class
            )
            .mapRequestType(command1, InternalRequest1.class)
            .mapRequestTypes(
                singletonMap(
                    command2,
                    InternalRequest2.class
                )
            )
            .mapTopicMessageType(
                topic1,
                command1,
                InternalMessage1.class
            )
            .mapTopicMessageTypes(
                topic2,
                singletonMap(command2, InternalMessage2.class)
            )
            .settingsBuilder()
            .parent()
            .settingsBuilder()
            .mapRequestType(command3, InternalRequest3.class)
            .mapTopicMessageType(topic3, command3, InternalMessage3.class)
            .parent()
            .settings(settings)
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .dataCodec(dataCodec)
            .entityCodec(entityCodec)
            .addSingleton(singleton1)
            .addSingleton("singleton2", singleton2)
            .addSingletons(Arrays.asList(singleton3, singleton4))
            .addSingletons(singletonMap("singleton5", singleton5))
            .beanContext(beanContext)
            .bindingContext(bindingContext)
            .scanAndAddAllBeans(scanAndAddAllBeans)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .textMessageDeserializer(textMessageDeserializer);

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "scanAndAddAllBeans"),
            scanAndAddAllBeans
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "dataCodec"),
            dataCodec
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "marshaller"),
            marshaller
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "unmarshaller"),
            unmarshaller
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "entityCodec"),
            entityCodec
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "bindingContext"),
            bindingContext
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "marshaller"),
            marshaller
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "messageSerializer"),
            messageSerializer
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "messageDeserializer"),
            messageDeserializer
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "textMessageDeserializer"),
            textMessageDeserializer
        );
        Set<String> packagesToScan = FieldUtil.getFieldValue(
            sut,
            "packagesToScan"
        );
        Asserts.assertEquals(
            packagesToScan,
            Arrays.asList(
                "com.tvd12.ezymq.common.test1",
                "com.tvd12.ezymq.common.test2",
                "com.tvd12.ezymq.common.test3",
                "com.tvd12.ezymq.common.test4",
                "com.tvd12.ezymq.common.test5",
                "com.tvd12.ezymq.common.test.codec"
            ),
            false
        );

        Map<String, Class> requestTypeByCommand = FieldUtil.getFieldValue(
            sut,
            "requestTypeByCommand"
        );
        Map<String, Map<String, Class>> messageTypeMapByTopic = FieldUtil.getFieldValue(
            sut,
            "messageTypeMapByTopic"
        );
        Asserts.assertEquals(
            requestTypeByCommand,
            EzyMapBuilder.mapBuilder()
                .put(command1, InternalRequest1.class)
                .put(command2, InternalRequest2.class)
                .toMap(),
            false
        );
        Asserts.assertEquals(
            messageTypeMapByTopic,
            EzyMapBuilder.mapBuilder()
                .put(
                    topic1,
                    EzyMapBuilder.mapBuilder()
                        .put(command1, InternalMessage1.class)
                        .build()
                )
                .put(
                    topic2,
                    EzyMapBuilder.mapBuilder()
                        .put(command2, InternalMessage2.class)
                        .build()
                )
                .toMap(),
            false
        );
        EzyBeanContextBuilder beanContextBuilder = FieldUtil.getFieldValue(
            sut,
            "beanContextBuilder"
        );
        EzySingletonFactory singletonFactory = FieldUtil.getFieldValue(
            beanContextBuilder,
            "singletonFactory"
        );
        Set<Object> singletonSet = FieldUtil.getFieldValue(
            singletonFactory,
            "singletonSet"
        );
        Asserts.assertTrue(
            singletonSet.containsAll(
                Sets.newHashSet(
                    singleton1,
                    singleton2,
                    singleton3,
                    singleton4,
                    singleton5,
                    singleton6
                )
            )
        );

        verify(beanContext, times(1)).getSingletonMapByKey();
    }

    @Test
    public void buildNotNullTest() {
        // given
        String command1 = RandomUtil.randomShortAlphabetString();
        String command2 = RandomUtil.randomShortAlphabetString();
        String command3 = RandomUtil.randomShortAlphabetString();
        String topic1 = RandomUtil.randomShortAlphabetString();
        String topic2 = RandomUtil.randomShortAlphabetString();
        String topic3 = RandomUtil.randomShortAlphabetString();
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        Object singleton1 = new Singleton1();
        Object singleton2 = new Singleton2();
        Object singleton3 = new Singleton3();
        Object singleton4 = new Singleton4();
        Object singleton5 = new Singleton5();
        Object singleton6 = new Singleton6();

        EzyBeanContext beanContext = mock(EzyBeanContext.class);
        when(
            beanContext.getSingletonMapByKey()
        ).thenReturn(
            singletonMap(
                EzyBeanKey.of(
                    "singleton6",
                    Object.class
                ),
                singleton6
            )
        );
        EzyBindingContext bindingContext = mock(EzyBindingContext.class);
        boolean scanAndAddAllBeans = RandomUtil.randomBoolean();
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        EzyMessageDeserializer textMessageDeserializer = mock(EzyMessageDeserializer.class);

        InternalMQRpcSettings settings = new InternalMQRpcSettings.Builder(null)
            .build();

        // when
        InternalMQProxy sut = new InternalMQRpcProxyBuilder()
            .scan("com.tvd12.ezymq.common.test1")
            .scan(
                "com.tvd12.ezymq.common.test2",
                "com.tvd12.ezymq.common.test3"
            )
            .scan(
                Arrays.asList(
                    "com.tvd12.ezymq.common.test4",
                    "com.tvd12.ezymq.common.test5"
                )
            )
            .entryClass(
                EzyMQBytesDataCodecTest.class
            )
            .mapRequestType(command1, InternalRequest1.class)
            .mapRequestTypes(
                singletonMap(
                    command2,
                    InternalRequest2.class
                )
            )
            .mapTopicMessageType(
                topic1,
                command1,
                InternalMessage1.class
            )
            .mapTopicMessageTypes(
                topic2,
                singletonMap(command2, InternalMessage2.class)
            )
            .settingsBuilder()
            .mapRequestType(command3, InternalRequest3.class)
            .mapTopicMessageType(topic3, command3, InternalMessage3.class)
            .parent()
            .settings(settings)
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .dataCodec(dataCodec)
            .entityCodec(entityCodec)
            .addSingleton(singleton1)
            .addSingleton("singleton2", singleton2)
            .addSingletons(Arrays.asList(singleton3, singleton4))
            .addSingletons(singletonMap("singleton5", singleton5))
            .beanContext(beanContext)
            .bindingContext(bindingContext)
            .scanAndAddAllBeans(scanAndAddAllBeans)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .textMessageDeserializer(textMessageDeserializer)
            .build();

        // then
    }

    private static class InternalMQRpcProxyBuilder extends EzyMQRpcProxyBuilder<
        InternalMQRpcSettings,
        InternalMQProxy,
        InternalMQRpcProxyBuilder
        > {

        @Override
        public InternalMQRpcSettings.Builder settingsBuilder() {
            return (InternalMQRpcSettings.Builder) super.settingsBuilder();
        }

        @Override
        protected InternalMQProxy newProxy() {
            return new InternalMQProxy(
                settings,
                dataCodec,
                entityCodec
            );
        }

        @Override
        public Class<?> getRequestInterceptorAnnotationClass() {
            return EzyTestInterceptor.class;
        }

        @Override
        public Class<?> getRequestHandlerAnnotationClass() {
            return EzyTestHandler.class;
        }

        @Override
        protected EzyMQRpcSettings.Builder newSettingBuilder() {
            return new InternalMQRpcSettings.Builder(this);
        }

        @Override
        protected Class<?> getMessageConsumerAnnotationClass() {
            return EzyTestConsumer.class;
        }
    }

    private static class InternalMQProxy extends EzyMQProxy<
        InternalMQRpcSettings,
        EzyMQDataCodec
        > {

        public InternalMQProxy(
            InternalMQRpcSettings settings,
            EzyMQDataCodec dataCodec,
            EzyEntityCodec entityCodec
        ) {
            super(settings, dataCodec, entityCodec);
        }

        @Override
        public void close() throws IOException {}
    }

    private static class InternalMQRpcSettings extends EzyMQRpcSettings {

        public InternalMQRpcSettings(
            Properties properties,
            Map<String, Class> requestTypeByCommand,
            Map<String, Map<String, Class>> messageTypeMapByTopic
        ) {
            super(properties, requestTypeByCommand, messageTypeMapByTopic);
        }

        public static class Builder extends EzyMQRpcSettings.Builder<
            InternalMQRpcSettings,
            EzyTestMQRequestInterceptor,
            EzyTestMQRequestHandler,
            Builder
            > {

            public Builder(EzyMQRpcProxyBuilder parent) {
                super(parent);
            }

            @Override
            protected String getRequestCommand(Object handler) {
                return handler
                    .getClass()
                    .getAnnotation(EzyTestHandler.class)
                    .command();
            }

            @Override
            protected EzyConsumerAnnotationProperties getConsumerAnnotationProperties(
                EzyMQMessageConsumer messageConsumer
            ) {
                EzyTestConsumer anno = messageConsumer
                    .getClass()
                    .getAnnotation(EzyTestConsumer.class);
                return new EzyConsumerAnnotationProperties(
                    anno.topic(),
                    anno.command()
                );
            }

            @Override
            public InternalMQRpcSettings build() {
                return new InternalMQRpcSettings(
                    properties,
                    requestTypeByCommand,
                    messageTypeMapByTopic
                );
            }

            @Override
            public InternalMQRpcProxyBuilder parent() {
                return (InternalMQRpcProxyBuilder) super.parent();
            }
        }
    }

    private static class InternalRequest1 {}

    private static class InternalRequest2 {}

    private static class InternalRequest3 {}

    private static class InternalMessage1 {}

    private static class InternalMessage2 {}

    private static class InternalMessage3 {}

    private static class Singleton1 {}

    private static class Singleton2 {}

    private static class Singleton3 {}

    private static class Singleton4 {}

    private static class Singleton5 {}

    private static class Singleton6 {}
}
