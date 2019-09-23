package com.datasphere.runtime.compiler.stmts;

import java.io.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.utils.*;
import java.util.*;

public class MappedStream implements Serializable
{
    public String streamName;
    public final Map<String, Object> mappingProperties;
    
    public MappedStream() {
        this.streamName = null;
        this.mappingProperties = null;
    }
    
    public MappedStream(final String stream, final List<Property> map_props) {
        this.streamName = stream;
        this.mappingProperties = this.combineProperties(map_props);
    }
    
    @Override
    public String toString() {
        return this.streamName + " MAP(" + this.mappingProperties.toString() + ")";
    }
    
    private Map<String, Object> combineProperties(final List<Property> propList) {
        final Map<String, Object> props = Factory.makeCaseInsensitiveMap();
        if (propList != null) {
            for (final Property p : propList) {
                if (p.value instanceof List) {
                    final Object value = this.combineProperties((List<Property>)p.value);
                    props.put(p.name, value);
                }
                else {
                    props.put(p.name, p.value);
                }
            }
        }
        return props;
    }
}
