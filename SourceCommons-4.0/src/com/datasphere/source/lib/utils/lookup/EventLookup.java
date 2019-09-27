package com.datasphere.source.lib.utils.lookup;

import com.datasphere.event.*;
import java.util.*;

public interface EventLookup
{
    List<Object> get(final Event p0) throws Exception;
}
