package com.datasphere.anno;

import java.lang.annotation.*;
/*
 * 判断是否用 Derby
 */
public @interface EntryPoint {
    public static final int USEDBY_UI = 1;
    public static final int USEDBY_WACTIONSTORE = 2;
    
    int usedBy();
}
