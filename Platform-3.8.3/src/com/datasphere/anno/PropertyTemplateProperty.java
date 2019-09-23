package com.datasphere.anno;

import java.lang.annotation.*;
/*
 * 属性模板:滞留策略
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyTemplateProperty {
    String name();
    
    Class<?> type();
    
    boolean required();
    
    String defaultValue();
    
    String label() default "";
    
    String description() default "";
}
