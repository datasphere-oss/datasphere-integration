package com.datasphere.hdstore;

import com.fasterxml.jackson.databind.*;
import com.datasphere.hd.*;
import com.datasphere.hdstore.exceptions.*;

public interface HDQuery
{
    JsonNode getQuery();
    
    HDStore getHDStore(final int p0);
    
    HDQueryResult execute() throws CapabilityException;
}
