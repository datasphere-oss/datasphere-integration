package com.datasphere.cache;

import java.io.*;
/*
 * 过滤器用于匹配键值对
 */
public interface Filter<K, V> extends Serializable
{
    boolean matches(final K p0, final V p1);
}
