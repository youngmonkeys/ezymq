package com.tvd12.ezymq.kafka.annotation;

import java.lang.annotation.*;

/**
 * Indicates that a class handle a server event.
 *
 * @author tavandung12
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyKafkaHandler {

    /**
     * topic name.
     *
     * @return the topic name
     */
    String topic();

    /**
     * command name.
     *
     * @return server event name
     */
    String command() default "";
}
