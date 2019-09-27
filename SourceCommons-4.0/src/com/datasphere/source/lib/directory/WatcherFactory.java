package com.datasphere.source.lib.directory;

import java.util.*;

import com.datasphere.source.lib.prop.*;

import java.io.*;

public class WatcherFactory
{
    public static String WATCHER_INST;
    
    public static Watcher createWatcher(final Map<String, Object> map, final WatcherCallback callback) throws IOException {
        return createWatcher(new Property(map), callback);
    }
    
    public static Watcher createWatcher(final Property prop, final WatcherCallback callback) throws IOException {
        Watcher watcher = null;
        watcher = (Watcher)prop.getObject(WatcherFactory.WATCHER_INST, null);
        if (watcher != null) {
            return watcher;
        }
        if (prop.getBoolean(Property.NETWORK_FILE_SYSTEM, false)) {
            watcher = new NFSWatcher(prop, callback);
        }
        else {
            watcher = new LocalDirectoryWatcher(prop, callback);
        }
        return watcher;
    }
    
    static {
        WatcherFactory.WATCHER_INST = "watcherinst";
    }
}
