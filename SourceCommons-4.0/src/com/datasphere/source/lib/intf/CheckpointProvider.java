package com.datasphere.source.lib.intf;

import com.datasphere.recovery.*;

public interface CheckpointProvider
{
    CheckpointDetail getCheckpointDetail();
    
    Position getPositionDetail();
}
