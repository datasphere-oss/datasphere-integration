package com.datasphere.TypeHandlers;

class StringToLongVarCharHandler extends StringHandler
{
    public StringToLongVarCharHandler(final String targetType) {
        super(-16, targetType);
    }
}
