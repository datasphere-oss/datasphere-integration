package com.datasphere.TypeHandlers;

class ShortToIntegerHandler extends IntegerHandler
{
    public ShortToIntegerHandler(final String targetType) {
        super(4, Short.class, targetType);
    }
}
