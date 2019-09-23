package com.datasphere.appmanager;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;

public class AppManagerServerLocation
{
    public static final String APP_TO_APPMANAGER_MAP = "#AppToAppmanagerMap";
    private static Logger logger;
    private static IMap<Long, UUID> appToAppManagerSrvIdMap;
    private static Long staticAppId;
    
    public static String registerListerForAppManagerUpdate(final EntryListener<Long, UUID> entryListener) {
        return AppManagerServerLocation.appToAppManagerSrvIdMap.addEntryListener((EntryListener)entryListener, true);
    }
    
    public static void unRegisterListerForAppManagerUpdate(final String id) {
        AppManagerServerLocation.appToAppManagerSrvIdMap.removeEntryListener(id);
    }
    
    public static UUID getAppManagerServerId() {
        return (UUID)AppManagerServerLocation.appToAppManagerSrvIdMap.get((Object)AppManagerServerLocation.staticAppId);
    }
    
    public static void setAppManagerServerId(final UUID serverId) {
        if (AppManagerServerLocation.logger.isInfoEnabled() && AppManagerServerLocation.appToAppManagerSrvIdMap.size() > 0) {
            AppManagerServerLocation.logger.info((Object)("Current AppManager is " + AppManagerServerLocation.appToAppManagerSrvIdMap.get((Object)AppManagerServerLocation.staticAppId)));
        }
        AppManagerServerLocation.logger.info((Object)("Setting new AppManager in map " + serverId));
        AppManagerServerLocation.appToAppManagerSrvIdMap.put(AppManagerServerLocation.staticAppId, serverId);
        if (AppManagerServerLocation.logger.isInfoEnabled() && AppManagerServerLocation.appToAppManagerSrvIdMap.size() > 0) {
            AppManagerServerLocation.logger.info((Object)("New AppManager is " + AppManagerServerLocation.appToAppManagerSrvIdMap.get((Object)AppManagerServerLocation.staticAppId)));
        }
    }
    
    public static void removeAppManagerServerId(final UUID serverId) {
        AppManagerServerLocation.logger.info((Object)("AppManager node for application " + serverId + " is dead so removing it as AppManager"));
        AppManagerServerLocation.appToAppManagerSrvIdMap.remove((Object)AppManagerServerLocation.staticAppId, (Object)serverId);
        if (AppManagerServerLocation.logger.isInfoEnabled() && AppManagerServerLocation.appToAppManagerSrvIdMap.size() > 0) {
            AppManagerServerLocation.logger.info((Object)("New AppManager is " + AppManagerServerLocation.appToAppManagerSrvIdMap.get((Object)AppManagerServerLocation.staticAppId)));
        }
    }
    
    static {
        AppManagerServerLocation.logger = Logger.getLogger((Class)AppManagerServerLocation.class);
        AppManagerServerLocation.appToAppManagerSrvIdMap = HazelcastSingleton.get().getMap("#AppToAppmanagerMap");
        AppManagerServerLocation.staticAppId = new Long(1L);
    }
}
