package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.runtime.*;

public class AdapterDescription
{
    private final String adapterTypeName;
    private final String version;
    private final List<Property> props;
    
    public AdapterDescription(final String adapterTypeName, final List<Property> props) {
        this(adapterTypeName, null, props);
    }
    
    public AdapterDescription(final String adapterTypeName, final String version, final List<Property> props) {
        this.adapterTypeName = adapterTypeName;
        this.version = version;
        this.props = props;
    }
    
    public String getAdapterTypeName() {
        return this.adapterTypeName;
    }
    
    public String getVersion() {
        return this.version;
    }
    
    public List<Property> getProps() {
        return this.props;
    }
}
