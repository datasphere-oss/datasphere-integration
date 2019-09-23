package com.datasphere.runtime.compiler.custom;

import java.lang.annotation.*;

@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface AggHandlerDesc {
    Class<?> handler();
    
    Class<?> distinctHandler() default Object.class;
    
    String getMethod() default "getAggValue";
    
    String incMethod() default "incAggValue";
    
    String decMethod() default "decAggValue";
}
