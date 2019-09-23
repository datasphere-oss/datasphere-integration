package com.datasphere.runtime.containers;

import com.datasphere.proc.events.*;
import com.datasphere.runtime.components.*;

public class DynamicEventWrapper
{
    public final DynamicEvent event;
    public final TranslatedSchema schema;
    
    public DynamicEventWrapper(final DynamicEvent event, final TranslatedSchema schema) {
        this.event = event;
        this.schema = schema;
    }
}
