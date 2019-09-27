package com.datasphere.utils.writers.common;

import com.datasphere.recovery.*;

public interface Checkpointer
{
    void updatePosition(final Position p0);
    
    Position getAckPosition() throws Exception;
}
