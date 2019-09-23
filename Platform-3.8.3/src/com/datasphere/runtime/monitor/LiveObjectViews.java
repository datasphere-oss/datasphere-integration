package com.datasphere.runtime.monitor;

import java.util.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.channels.*;

public interface LiveObjectViews
{
    Collection<FlowComponent> getAllObjectsView();
    
    Collection<Channel> getAllChannelsView();
}
