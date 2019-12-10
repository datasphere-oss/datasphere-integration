package com.datasphere.source.lib.utils.lookup;

import com.datasphere.event.*;
import com.datasphere.proc.events.*;

import java.util.*;

public class HDEventMetadataLookup extends MetadataLookup
{
    public HDEventMetadataLookup(final String dataToBeLookedUp) {
        super(dataToBeLookedUp);
    }
    
    @Override
    public List<Object> get(final Event event) {
        final HDEvent hdEvent = (HDEvent)event;
        if (hdEvent.metadata == null) {
            throw new RuntimeException("metadata field of HDEvent to be looked up is not intialized");
        }
        if (!hdEvent.metadata.containsKey(this.dataToBeLookedUp)) {
            throw new RuntimeException("metadata field of HDEvent to be looked up doesn't contain specified key " + this.dataToBeLookedUp);
        }
        final List<Object> dataList = new ArrayList<Object>();
        dataList.add(hdEvent.metadata.get(this.dataToBeLookedUp));
        return dataList;
    }
}
