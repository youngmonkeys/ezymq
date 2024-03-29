package com.tvd12.ezymq.activemq.util;

import javax.jms.BytesMessage;
import javax.jms.Message;
import java.util.Enumeration;

public final class EzyActiveMessages {

    private EzyActiveMessages() {}

    public static byte[] getMessageBody(
        BytesMessage message
    ) throws Exception {
        int length = (int) message.getBodyLength();
        byte[] body = new byte[length];
        message.readBytes(body);
        return body;
    }

    public static void setMessageProperties(
        Message message,
        EzyActiveProperties props
    ) throws Exception {
        message.setJMSType(props.getType());
        message.setJMSCorrelationID(props.getCorrelationId());
        for (String key : props.keySet()) {
            Object value = props.getValue(key);
            if (value instanceof Integer) {
                message.setIntProperty(key, (Integer) value);
            } else if (value instanceof String) {
                message.setStringProperty(key, (String) value);
            } else if (value instanceof Boolean) {
                message.setBooleanProperty(key, (Boolean) value);
            } else if (value instanceof Byte) {
                message.setByteProperty(key, (Byte) value);
            } else if (value instanceof Double) {
                message.setDoubleProperty(key, (Double) value);
            } else if (value instanceof Float) {
                message.setFloatProperty(key, (Float) value);
            } else if (value instanceof Long) {
                message.setLongProperty(key, (Long) value);
            } else if (value instanceof Short) {
                message.setShortProperty(key, (Short) value);
            } else {
                message.setObjectProperty(key, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static EzyActiveProperties getMessageProperties(
        Message message
    ) throws Exception {
        EzyActiveProperties.Builder builder = EzyActiveProperties.builder()
            .type(message.getJMSType())
            .correlationId(message.getJMSCorrelationID());
        Enumeration<String> propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement();
            Object value = message.getObjectProperty(name);
            builder.addProperty(name, value);
        }
        return builder.build();
    }
}
