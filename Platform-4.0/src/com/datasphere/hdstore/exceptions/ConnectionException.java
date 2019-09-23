package com.datasphere.hdstore.exceptions;

public class ConnectionException extends HDStoreException
{
    private static final long serialVersionUID = -6132114902661917869L;
    private static final String MSG_EXC_MESSAGE = "Provider '%s' cannot connect to instance '%s': %s";
    public final Exception providerException;
    
    public ConnectionException(final String providerName, final String instanceName, final Exception providerException) {
        super(String.format("Provider '%s' cannot connect to instance '%s': %s", providerName, instanceName, providerException.getMessage()));
        this.providerException = providerException;
    }
}
