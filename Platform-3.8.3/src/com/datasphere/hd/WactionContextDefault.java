package com.datasphere.hd;

import org.apache.log4j.*;

public class HDContextDefault extends HDContext
{
    private static final long serialVersionUID = 2930824976792265751L;
    private static transient Logger logger;
    
    @Override
    public Object get(final Object key) {
        return null;
    }
    
    @Override
    public Object put(final String key, final Object value) {
        return null;
    }
    
    static {
        HDContextDefault.logger = Logger.getLogger((Class)HDContextDefault.class);
    }
}
