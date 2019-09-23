package com.datasphere.exceptionhandling;

import java.util.*;

public final class FixedSizeQueue<E> extends LinkedList<E>
{
    private static final long serialVersionUID = -3499341921349077573L;
    private int limit;
    
    public FixedSizeQueue(final int limit) {
        this.limit = limit;
    }
    
    @Override
    public boolean add(final E e) {
        super.add(e);
        while (this.size() > this.limit) {
            super.remove();
        }
        return true;
    }
}
