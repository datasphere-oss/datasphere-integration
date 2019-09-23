package com.datasphere.metaRepository.actions;

public class ActionablePropertiesFactory
{
    public TextProperties createTextProperties() {
        return new TextProperties();
    }
    
    public ObjectProperties createObjectProperties() {
        return new ObjectProperties();
    }
    
    public MetaObjectProperties createMetaObjectProperties() {
        return new MetaObjectProperties();
    }
    
    public BooleanProperties createBooleanProperties() {
        return new BooleanProperties();
    }
    
    public EnumProperties createEnumProperties() {
        return new EnumProperties();
    }
    
    public NumberProperties createNumberProperties() {
        return new NumberProperties();
    }
}
