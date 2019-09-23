package com.datasphere.runtime.components;

import com.datasphere.runtime.channels.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.meta.*;

public abstract class IWindow extends FlowComponent implements PubSub, Channel.NewSubscriberAddedCallback, Restartable, Compound
{
    public IWindow(final BaseServer srv, final MetaInfo.MetaObject info) {
        super(srv, info);
    }
}
