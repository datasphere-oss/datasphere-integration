package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;

public interface ActionHandler
{
    MetaInfo.StatusInfo.Status handle(final AppContext p0, final Event p1, final ArrayList<UUID> p2) throws Exception;
}
