package com.datasphere.anno;

import java.lang.annotation.*;
/*
 * 定义数据类型
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface EventType {
    String schema();
    
    String classification();
    
    String uri();
}
