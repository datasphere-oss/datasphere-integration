package com.datasphere.runtime.components;

import com.datasphere.event.*;
import com.datasphere.recovery.*;

public interface ReceiptCallback
{
    void ack(final int p0, final Position p1);
    
    void bytesWritten(final long p0);
    
    void latency(final long p0);
    
    void notifyException(final Exception p0, final Event p1);
    
    void gracefulStopApplication(final Event p0);
}
