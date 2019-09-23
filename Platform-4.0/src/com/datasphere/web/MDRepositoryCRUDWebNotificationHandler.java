package com.datasphere.web;

import com.datasphere.runtime.*;
import com.datasphere.runtime.meta.*;
import org.apache.log4j.*;
import com.hazelcast.core.*;
import com.datasphere.web.api.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import org.apache.http.*;
import java.util.concurrent.*;
import java.util.*;

public class MDRepositoryCRUDWebNotificationHandler implements HazelcastIMapListener<String, MetaInfo.MetaObject>
{
    private static Logger logger;
    private final String BASE_EXCEPTION = "Couldn't signal frontend when new MetaObject was ";
    private final String ON_ADD;
    private final String ON_UPDATE;
    private final String ON_REMOVE;
    
    public MDRepositoryCRUDWebNotificationHandler() {
        this.ON_ADD = "Couldn't signal frontend when new MetaObject was ".concat("added");
        this.ON_UPDATE = "Couldn't signal frontend when new MetaObject was ".concat("updated");
        this.ON_REMOVE = "Couldn't signal frontend when new MetaObject was ".concat("removed");
    }
    
    public void entryAdded(final EntryEvent<String, MetaInfo.MetaObject> entryEvent) {
    }
    
    public void entryRemoved(final EntryEvent<String, MetaInfo.MetaObject> entryEvent) {
    }
    
    public void entryUpdated(final EntryEvent<String, MetaInfo.MetaObject> entryEvent) {
    }
    
    private void notifyFrontendClient(final EntryEvent<String, MetaInfo.MetaObject> entryEvent, final String errorMessage, final ClientOperations.CRUD crud) {
        final ConcurrentHashMap<RMIWebSocket, Boolean> members = WebServer.getMembers();
        for (final RMIWebSocket ws : members.keySet()) {
            try {
                if (ws.userAuthToken == null || !PermissionUtility.checkPermission((MetaInfo.MetaObject)entryEvent.getValue(), ObjectPermission.Action.read, ws.userAuthToken, false)) {
                    continue;
                }
                final Map<String, String> result = new HashMap<String, String>(3);
                result.put("id", ((MetaInfo.MetaObject)entryEvent.getValue()).getFQN());
                result.put("type", ((MetaInfo.MetaObject)entryEvent.getValue()).getType().toString());
                if (((MetaInfo.MetaObject)entryEvent.getValue()).getMetaInfoStatus().isDropped()) {
                    result.put("CRUDOp", String.valueOf(ClientOperations.CRUD.DELETE));
                }
                else {
                    result.put("CRUDOp", String.valueOf(crud));
                }
                ws.call(true, "metadata_updated", result);
            }
            catch (MetaDataRepositoryException e) {
                MDRepositoryCRUDWebNotificationHandler.logger.warn((Object)errorMessage, (Throwable)e);
            }
            catch (ConnectionClosedException e2) {
                MDRepositoryCRUDWebNotificationHandler.logger.warn((Object)errorMessage, (Throwable)e2);
            }
            catch (RMIWebSocketException e3) {
                MDRepositoryCRUDWebNotificationHandler.logger.warn((Object)errorMessage, (Throwable)e3);
            }
        }
    }
    
    static {
        MDRepositoryCRUDWebNotificationHandler.logger = Logger.getLogger((Class)MDRepositoryCRUDWebNotificationHandler.class);
    }
}
