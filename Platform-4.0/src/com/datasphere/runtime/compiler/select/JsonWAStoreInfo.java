package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.meta.*;

public abstract class JsonWAStoreInfo
{
    public abstract MetaInfo.WAStoreView getStoreView();
    
    public abstract MetaInfo.HDStore getStore();
    
    @Override
    public abstract String toString();
}
