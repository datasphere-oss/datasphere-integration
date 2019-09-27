package com.datasphere.Fetchers;

import java.util.*;

import com.datasphere.proc.events.*;
import com.datasphere.utils.writers.common.*;

public class EnvFetcher extends Fetcher
{
    String envVariableName;
    public static Map<String, String> env;
    
    public EnvFetcher(final String variableName) {
        this.envVariableName = variableName;
    }
    
    @Override
    public Object fetch(final HDEvent event) {
        return EnvFetcher.env.get(this.envVariableName);
    }
    
    @Override
    public Object fetch(final EventDataObject event) {
        return EnvFetcher.env.get(this.envVariableName);
    }
    
    @Override
    public String toString() {
        return "ENV(" + this.envVariableName + ")";
    }
    
    static {
        EnvFetcher.env = System.getenv();
    }
}
