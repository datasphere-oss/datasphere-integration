package com.datasphere.Fetchers;

import com.datasphere.proc.events.*;
import com.datasphere.utils.writers.common.*;

public abstract class Fetcher
{
    public abstract Object fetch(final HDEvent p0);
    
    public abstract Object fetch(final EventDataObject p0);
    
    public Object fetch(final HDEvent event, final boolean before) {
        return null;
    }
    
    public Object fetch(final EventDataObject event, final boolean before) {
        return null;
    }
    
    @Override
    public String toString() {
        return "";
    }
    
    protected String fetchArgument(final String key) {
        if (key == null || key.isEmpty()) {
            return key;
        }
        final String part1 = key.substring(key.indexOf(40) + 1, key.indexOf(41));
        String part2 = part1.trim();
        if (part2.startsWith("'")) {
            part2 = part2.substring(1, part2.length() - 1);
        }
        return part2;
    }
}
