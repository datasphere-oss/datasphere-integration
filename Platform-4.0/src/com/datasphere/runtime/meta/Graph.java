package com.datasphere.runtime.meta;

import java.util.*;

public class Graph<UUID, Set> extends HashMap<UUID, Set>
{
    @Override
    public Set get(final Object key) {
        synchronized (key) {
            if (key == null) {
                return null;
            }
            if (super.get(key) == null) {
                super.put((UUID)key, (Set)new LinkedHashSet());
            }
            return super.get(key);
        }
    }
}
