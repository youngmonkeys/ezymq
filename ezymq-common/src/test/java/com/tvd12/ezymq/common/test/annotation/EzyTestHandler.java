package com.tvd12.ezymq.common.test.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyTestHandler {

    String value() default "";

    String command() default "";
}
