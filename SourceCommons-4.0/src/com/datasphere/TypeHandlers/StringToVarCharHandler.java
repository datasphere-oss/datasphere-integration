package com.datasphere.TypeHandlers;

class StringToVarCharHandler extends StringHandler
{
    public StringToVarCharHandler(final String targetType) {
        super(12, targetType);
    }
}
