package com.tvd12.ezymq.activemq.annotation;

import java.lang.annotation.*;

/**
 * Indicates that a class handle a server event.
 *
 * @author tavandung12
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyActiveRequestHandle {

    /**
     * command name.
     *
     * @return the event name
     */
    String value() default "";

    /**
     * command name.
     *
     * @return server event name
     */
    String cmd() default "";
}
