package com.datasphere.runtime.components;

import com.datasphere.runtime.*;
import com.datasphere.runtime.meta.*;

public abstract class IStreamGenerator extends FlowComponent implements Publisher, Runnable, Restartable
{
    public IStreamGenerator(final BaseServer srv, final MetaInfo.MetaObject info) {
        super(srv, info);
    }
}
