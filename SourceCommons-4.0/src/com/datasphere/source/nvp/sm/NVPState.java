package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.intf.*;

public abstract class NVPState implements State
{
    public abstract Constant.status canAccept(final char p0);
}
