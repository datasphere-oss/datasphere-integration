package org.jctools.util;

public final class UnsafeRefArrayAccess
{
    public static final <E> void spElement(final E[] buffer, final long offset, final E e) {
        UnsafeAccess.UNSAFE.putObject(buffer, offset, e);
    }
    
    public static final <E> void soElement(final E[] buffer, final long offset, final E e) {
        UnsafeAccess.UNSAFE.putOrderedObject(buffer, offset, e);
    }
    
    public static final <E> E lpElement(final E[] buffer, final long offset) {
        return (E)UnsafeAccess.UNSAFE.getObject(buffer, offset);
    }
    
    public static final <E> E lvElement(final E[] buffer, final long offset) {
        return (E)UnsafeAccess.UNSAFE.getObjectVolatile(buffer, offset);
    }
}
