package com.datasphere.TypeHandlers;

class BooleanToBitHandler extends BooleanToIntegerHandler
{
    public BooleanToBitHandler(final String targetType) {
        super(-7, targetType);
    }
}
