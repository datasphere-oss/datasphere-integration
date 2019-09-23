package com.datasphere.hdstore.exceptions;

public class HDStoreMissingException extends HDStoreException
{
    private static final long serialVersionUID = -5031853353405227519L;
    private static final String MSG_EXC_MESSAGE = "HDStore '%s' cannot be found by provider '%s', instance '%s'";
    public final String hdStoreName;
    
    public HDStoreMissingException(final String providerName, final String instanceName, final String hdStoreName) {
        super(String.format("HDStore '%s' cannot be found by provider '%s', instance '%s'", hdStoreName, providerName, instanceName));
        this.hdStoreName = hdStoreName;
    }
}
