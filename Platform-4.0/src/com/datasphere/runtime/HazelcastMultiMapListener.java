package com.datasphere.runtime;

import com.hazelcast.core.*;

public interface HazelcastMultiMapListener<K, V> extends EntryListener<K, V>
{
}
