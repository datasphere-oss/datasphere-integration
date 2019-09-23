package com.datasphere.runtime.compiler.custom;

import java.lang.annotation.*;

@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface AcceptWildcard {
}
