package com.datasphere.TypeHandlers;

class ByteToIntegerHandler extends IntegerHandler
{
    public ByteToIntegerHandler(final String targetType) {
        super(4, Byte.class, targetType);
    }
}
