package com.datasphere.source.lib.utils;

class CollectionFieldModifier extends FieldModifier
{
    private String collectionDelimiter;
    
    CollectionFieldModifier(final String collectionDelimiter) {
        this.collectionDelimiter = ",";
        if (collectionDelimiter != null && !collectionDelimiter.trim().isEmpty()) {
            this.collectionDelimiter = collectionDelimiter;
        }
    }
    
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) {
        String value = fieldValue.toString();
        value = value.replaceAll("\\[", "");
        value = value.replaceAll("\\]", "");
        if (!this.collectionDelimiter.equals(",")) {
            value = value.replaceAll(",", this.collectionDelimiter);
        }
        return value;
    }
}
