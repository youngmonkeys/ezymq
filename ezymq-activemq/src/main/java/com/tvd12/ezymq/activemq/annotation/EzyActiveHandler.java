package com.tvd12.ezymq.activemq.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyActiveHandler {

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
    String command() default "";
}
