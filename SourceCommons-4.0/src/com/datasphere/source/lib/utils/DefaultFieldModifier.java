package com.datasphere.source.lib.utils;

class DefaultFieldModifier extends FieldModifier
{
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) {
        return fieldValue.toString();
    }
}
