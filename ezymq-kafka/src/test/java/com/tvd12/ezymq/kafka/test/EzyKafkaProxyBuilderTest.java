package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.EzyKafkaProxyBuilder;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import org.testng.annotations.Test;

public class EzyKafkaProxyBuilderTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyKafkaProxyBuilder sut = EzyKafkaProxy.builder()
            .mapMessageType("topic1", Integer.class)
            .mapMessageType("topic1", "command1", String.class)
            .mapMessageTypes(
                "topic1",
                EzyMapBuilder.mapBuilder()
                    .put("byte", Byte.class)
                    .put("boolean", Boolean.class)
                    .toMap()
            )
            .mapMessageTypes(
                EzyMapBuilder.mapBuilder()
                    .put(
                        "topic2",
                        EzyMapBuilder.mapBuilder()
                            .put("char", Character.class)
                            .put("double", Double.class)
                            .toMap()
                    )
                    .toMap()
            )
            .settingsBuilder()
            .parent();

        // when
        sut.build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(
                sut,
                "messageTypesByTopic"
            ),
            EzyMapBuilder.mapBuilder()
                .put(
                    "topic1",
                    EzyMapBuilder.mapBuilder()
                        .put("", Integer.class)
                        .put("command1", String.class)
                        .put("byte", Byte.class)
                        .put("boolean", Boolean.class)
                        .toMap()
                )
                .put(
                    "topic2",
                    EzyMapBuilder.mapBuilder()
                        .put("char", Character.class)
                        .put("double", Double.class)
                        .toMap()
                )
                .toMap()
        );
    }
}
