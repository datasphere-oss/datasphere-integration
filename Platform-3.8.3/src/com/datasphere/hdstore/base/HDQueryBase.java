package com.datasphere.hdstore.base;

import com.datasphere.hdstore.*;
import com.fasterxml.jackson.databind.*;
import java.util.*;

public abstract class HDQueryBase<T extends HDStore> implements HDQuery
{
    private final JsonNode queryJson;
    private final List<T> hdStores;
    
    protected HDQueryBase(final JsonNode queryJson, final Collection<T> hdStores) {
        this.queryJson = queryJson;
        (this.hdStores = new ArrayList<T>(hdStores.size())).addAll((Collection<? extends T>)hdStores);
    }
    
    @Override
    public JsonNode getQuery() {
        return this.queryJson;
    }
    
    @Override
    public T getHDStore(final int index) {
        T result = null;
        if (index >= 0 && index < this.hdStores.size()) {
            result = this.hdStores.get(index);
        }
        return result;
    }
}
