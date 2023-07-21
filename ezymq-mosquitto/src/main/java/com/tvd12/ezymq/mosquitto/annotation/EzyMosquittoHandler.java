package com.tvd12.ezymq.mosquitto.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyMosquittoHandler {

    String value() default "";

    String command() default "";
}
