package com.datasphere.anno;

import java.lang.annotation.*;
/*
 * 属性模板
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyTemplate {
    String name();
    
    AdapterType type();
    
    String runtimeClass() default "<NOT SET>";
    
    PropertyTemplateProperty[] properties();
    
    Class<?> inputType() default NotSet.class;
    
    Class<?> outputType() default NotSet.class;
    
    boolean requiresParser() default false;
    
    boolean requiresFormatter() default false;
    
    String version() default "0.0.0";
    
    boolean isParallelizable() default false;
}
