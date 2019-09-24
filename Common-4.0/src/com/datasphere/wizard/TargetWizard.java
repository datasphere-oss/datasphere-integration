package com.datasphere.wizard;

import java.util.*;
/*
 * 目标端 向导创建接口
 */
public interface TargetWizard
{
    String validateConnection(final Map<String, Object> p0);
    
    String list(final Map<String, Object> p0);
}
