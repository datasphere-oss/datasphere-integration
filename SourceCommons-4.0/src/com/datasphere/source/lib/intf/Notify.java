package com.datasphere.source.lib.intf;

import java.io.*;

import com.datasphere.source.lib.constant.*;

public interface Notify
{
    void handleEvent(final Constant.eventType p0, final File p1) throws IOException;
}
