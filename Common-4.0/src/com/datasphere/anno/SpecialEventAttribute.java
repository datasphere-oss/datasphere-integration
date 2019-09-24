package com.datasphere.anno;

import java.lang.annotation.*;
/*
 * 定义特定数据类型的属性
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface SpecialEventAttribute {
}
