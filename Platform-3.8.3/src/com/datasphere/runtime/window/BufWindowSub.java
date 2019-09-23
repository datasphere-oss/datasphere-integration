package com.datasphere.runtime.window;

import com.datasphere.runtime.*;
import java.util.*;
import com.datasphere.runtime.containers.*;

public interface BufWindowSub
{
    void receive(final RecordKey p0, final Collection<DARecord> p1, final IBatch p2, final IBatch p3);
}
