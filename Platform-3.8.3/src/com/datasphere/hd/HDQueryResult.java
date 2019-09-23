package com.datasphere.hd;

import com.datasphere.hdstore.*;
import java.util.*;

public abstract class HDQueryResult implements Iterable<HD>
{
    public long size() {
        long count = 0L;
        final Iterator<HD> iterator = this.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            ++count;
        }
        return count;
    }
}
