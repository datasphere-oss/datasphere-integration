package com.datasphere.runtime;

import com.hazelcast.map.listener.*;

public interface HazelcastIMapListener<K, V> extends EntryAddedListener<K, V>, EntryRemovedListener<K, V>, EntryUpdatedListener<K, V>
{
}
