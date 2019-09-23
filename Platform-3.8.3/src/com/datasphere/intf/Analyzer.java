package com.datasphere.intf;

import java.util.*;
/*
 * 对适配器的配置文件进行解析的接口
 */
public interface Analyzer
{
    Map<String, Object> getFileDetails();
    
    List<Map<String, Object>> getProbableProperties();
}
