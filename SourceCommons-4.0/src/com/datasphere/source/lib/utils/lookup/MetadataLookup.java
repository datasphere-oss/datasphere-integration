package com.datasphere.source.lib.utils.lookup;

public abstract class MetadataLookup implements EventLookup
{
    protected String dataToBeLookedUp;
    
    public MetadataLookup(final String dataToBeLookedUp) {
        this.dataToBeLookedUp = null;
        this.dataToBeLookedUp = dataToBeLookedUp;
    }
}
