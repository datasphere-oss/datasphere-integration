package com.datasphere.runtime.compiler.stmts;

import com.datasphere.runtime.*;
import java.util.*;

public class ExceptionHandler
{
    public final List<Property> props;
    
    public ExceptionHandler(final List<Property> props) {
        final List<Property> transformedProps = new ArrayList<Property>();
        if (props != null && !props.isEmpty()) {
            for (final Property prop : props) {
                transformedProps.add(new Property(prop.name, (prop.value != null) ? ((String)prop.value).toUpperCase() : null));
            }
        }
        this.props = transformedProps;
    }
}
