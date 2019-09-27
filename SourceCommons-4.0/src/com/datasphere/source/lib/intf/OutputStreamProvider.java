package com.datasphere.source.lib.intf;

import java.io.*;

public interface OutputStreamProvider
{
    OutputStream provideOutputStream(final OutputStream p0) throws IOException;
}
