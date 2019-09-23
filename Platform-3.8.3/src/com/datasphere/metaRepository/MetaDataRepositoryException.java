package com.datasphere.metaRepository;

public class MetaDataRepositoryException extends Exception
{
    public MetaDataRepositoryException(final String s) {
        super(s);
    }
    
    public MetaDataRepositoryException(final String msg, final Throwable ex) {
        super(msg, ex);
    }
}
