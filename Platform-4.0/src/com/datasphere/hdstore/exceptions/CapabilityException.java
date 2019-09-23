package com.datasphere.hdstore.exceptions;

public class CapabilityException extends HDStoreException
{
    private static final long serialVersionUID = -6132114902661917869L;
    private static final String MSG_EXC_MESSAGE = "Capability '%s' not provided by HDStore provider '%s'";
    public final String capabilityViolated;
    
    public CapabilityException(final String providerName, final String capabilityViolated) {
        super(String.format("Capability '%s' not provided by HDStore provider '%s'", capabilityViolated, providerName));
        this.capabilityViolated = capabilityViolated;
    }
    
    public CapabilityException(final String capabilityViolated) {
        this("<Unknown>", capabilityViolated);
    }
}
