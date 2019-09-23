package com.datasphere.gen;

import org.eclipse.persistence.jpa.metadata.*;
import java.util.*;
import org.eclipse.persistence.logging.*;
import org.eclipse.persistence.internal.jpa.metadata.xml.*;

public class HMetadataSource extends XMLMetadataSource
{
    String storeName;
    
    public HMetadataSource() {
        this.storeName = "default";
    }
    
    public void setStoreName(final String storeName) {
        this.storeName = storeName;
    }
    
    public XMLEntityMappings getEntityMappings(final Map<String, Object> properties, final ClassLoader classLoader, final SessionLog log) {
        if (this.storeName == null || this.storeName.trim().length() == 0) {
            this.storeName = "default";
        }
        properties.put("eclipselink.metadata-source.xml.file", "eclipselink-orm-" + this.storeName + ".xml");
        properties.put("javax.persistence.validation.factory", null);
        return super.getEntityMappings((Map)properties, classLoader, log);
    }
}
