package com.datasphere.Fetchers;

import com.datasphere.proc.events.*;
import com.datasphere.utils.writers.common.*;

public class StaticValueFetcher extends Fetcher
{
    String staticText;
    
    public StaticValueFetcher(final String value) {
        this.staticText = value;
    }
    
    @Override
    public Object fetch(final HDEvent event) {
        return this.staticText;
    }
    
    @Override
    public Object fetch(final EventDataObject event) {
        return this.staticText;
    }
    
    @Override
    public String toString() {
        return "StaticTextFetcher {" + this.staticText + "}";
    }
}
