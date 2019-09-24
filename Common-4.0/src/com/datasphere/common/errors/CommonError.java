package com.datasphere.common.errors;

public enum CommonError implements IError
{
    JMX_ERROR("DA-JMX-1001", "Error while registering mbean"), 
    MD_PERMISSION_ERROR("DA-MD-1001", "Error while checking permission");
    
    public String type;
    public String text;
    
    private CommonError(final String type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
}
