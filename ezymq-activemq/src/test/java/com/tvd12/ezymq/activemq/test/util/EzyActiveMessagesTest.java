package com.tvd12.ezymq.activemq.test.util;

import com.tvd12.ezymq.activemq.util.EzyActiveMessages;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.Message;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class EzyActiveMessagesTest extends BaseTest {

    @Test
    public void setMessagePropertiesTest() throws Exception {
        // given
        String type = RandomUtil.randomShortAlphabetString();
        String correlationId = RandomUtil.randomShortAlphabetString();
        boolean booleanValue = RandomUtil.randomBoolean();
        byte byteValue = RandomUtil.randomByte();
        double doubleValue = RandomUtil.randomDouble();
        float floatValue = RandomUtil.randomFloat();
        int intValue = RandomUtil.randomInt();
        long longValue = RandomUtil.randomLong();
        short shortValue = RandomUtil.randomShort();
        String stringValue = RandomUtil.randomShortAlphabetString();
        Object objectValue = RandomUtil.randomShortByteArray();
        EzyActiveProperties props = EzyActiveProperties.builder()
            .type(type)
            .correlationId(correlationId)
            .addProperty("boolean", booleanValue)
            .addProperty("byte", byteValue)
            .addProperty("double", doubleValue)
            .addProperty("float", floatValue)
            .addProperty("int", intValue)
            .addProperty("long", longValue)
            .addProperty("short", shortValue)
            .addProperty("string", stringValue)
            .addProperty("object", objectValue)
            .build();

        // when
        Message message = mock(Message.class);
        EzyActiveMessages.setMessageProperties(message, props);

        // then
        verify(message, times(1)).setBooleanProperty(
            "boolean", booleanValue
        );
        verify(message, times(1)).setByteProperty(
            "byte", byteValue
        );
        verify(message, times(1)).setDoubleProperty(
            "double", doubleValue
        );
        verify(message, times(1)).setFloatProperty(
            "float", floatValue
        );
        verify(message, times(1)).setIntProperty(
            "int", intValue
        );
        verify(message, times(1)).setLongProperty(
            "long", longValue
        );
        verify(message, times(1)).setShortProperty(
            "short", shortValue
        );
        verify(message, times(1)).setStringProperty(
            "string", stringValue
        );
        verify(message, times(1)).setObjectProperty(
            "object", objectValue
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getMessagePropertiesTest() throws Exception {
        // given
        Message message = mock(Message.class);
        String type = RandomUtil.randomShortAlphabetString();
        when(message.getJMSType()).thenReturn(type);

        String correlationId = RandomUtil.randomShortAlphabetString();
        when(message.getJMSCorrelationID()).thenReturn(correlationId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(message.getPropertyNames()).thenReturn(propertyNames);

        AtomicInteger count = new AtomicInteger();
        when(propertyNames.hasMoreElements()).thenAnswer(it ->
            count.incrementAndGet() == 1
        );
        String propertyName = RandomUtil.randomShortAlphabetString();
        when(propertyNames.nextElement()).thenReturn(propertyName);

        Object value = RandomUtil.randomShortAlphabetString();
        when(message.getObjectProperty(propertyName)).thenReturn(value);

        // when
        EzyActiveProperties actual = EzyActiveMessages.getMessageProperties(
            message
        );

        // then
        Asserts.assertEquals(
            actual,
            EzyActiveProperties.builder()
                .type(type)
                .correlationId(correlationId)
                .addProperty(propertyName, value)
                .build()
        );
    }

    @Override
    public Class<?> getTestClass() {
        return EzyActiveMessagesTest.class;
    }
}
