package com.datasphere.web;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.http.ConnectionClosedException;
import org.apache.log4j.Logger;

import com.hazelcast.core.EntryEvent;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.ObjectPermission;
import com.datasphere.uuid.UUID;

public class MetaObjectStatusChangeWebNotificationHandler implements HazelcastIMapListener<String, MetaInfo.StatusInfo>
{
    private static Logger logger;
    private MDRepository mdr;
    private final String BASE_EXCEPTION = "Couldn't signal frontend when status changed for MetaObject with UUID: ";
    
    public MetaObjectStatusChangeWebNotificationHandler() {
        this.mdr = MetadataRepository.getINSTANCE();
    }
    
    public void entryAdded(final EntryEvent<String, MetaInfo.StatusInfo> entryEvent) {
        this.notifyFrontendClient(entryEvent);
    }
    
    public void entryRemoved(final EntryEvent<String, MetaInfo.StatusInfo> entryEvent) {
        this.notifyFrontendClient(entryEvent);
    }
    
    public void entryUpdated(final EntryEvent<String, MetaInfo.StatusInfo> entryEvent) {
        this.notifyFrontendClient(entryEvent);
    }
    
    private void notifyFrontendClient(final EntryEvent<String, MetaInfo.StatusInfo> entryEvent) {
        final Map<RMIWebSocket, Boolean> members = WebServer.getMembers();
        final Set<RMIWebSocket> keysSet = members.keySet();
        for (final RMIWebSocket ws : keysSet) {
            try {
                if (ws.userAuthToken == null || !PermissionUtility.checkPermission(((MetaInfo.StatusInfo)entryEvent.getValue()).getOID(), ObjectPermission.Action.read, ws.userAuthToken, false)) {
                    continue;
                }
                final Map<String, Object> result = new HashMap<String, Object>(3);
                final MetaInfo.MetaObject metaObject = this.mdr.getMetaObjectByUUID(((MetaInfo.StatusInfo)entryEvent.getValue()).getOID(), ws.userAuthToken);
                if (metaObject != null) {
                    if ((!metaObject.getType().equals(EntityType.APPLICATION) && !metaObject.getType().equals(EntityType.FLOW)) || metaObject.getMetaInfoStatus().isAnonymous() || metaObject.getMetaInfoStatus().isAdhoc()) {
                        continue;
                    }
                    result.put("id", metaObject.getFQN());
                    result.put("type", metaObject.getType().toString());
                    result.put("previousStatus", ((MetaInfo.StatusInfo)entryEvent.getValue()).getPreviousStatus().toString());
                    result.put("currentStatus", ((MetaInfo.StatusInfo)entryEvent.getValue()).getStatus().toString());
                    final MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)metaObject;
                    final Iterator<UUID> componentIterator = flowMetaObject.getAllObjects().iterator();
                    final Set<String> allComponentsInApp = new HashSet<String>(flowMetaObject.getAllObjects().size());
                    while (componentIterator.hasNext()) {
                        final MetaInfo.MetaObject mob = this.mdr.getMetaObjectByUUID(componentIterator.next(), ws.userAuthToken);
                        if (mob != null) {
                            allComponentsInApp.add(mob.getFQN());
                        }
                    }
                    if (allComponentsInApp != null) {
                        result.put("objects", allComponentsInApp);
                    }
                    ws.call(true, "status_change", result);
                }
                else {
                    MetaObjectStatusChangeWebNotificationHandler.logger.warn((Object)("Callback called on status change but couldn't find MetaObject with UUID: " + ((MetaInfo.StatusInfo)entryEvent.getValue()).getOID()));
                }
            }
            catch (MetaDataRepositoryException e) {
                MetaObjectStatusChangeWebNotificationHandler.logger.warn((Object)"Couldn't signal frontend when status changed for MetaObject with UUID: ".concat(((MetaInfo.StatusInfo)entryEvent.getValue()).getOID().toString()), (Throwable)e);
            }
            catch (ConnectionClosedException e2) {
                MetaObjectStatusChangeWebNotificationHandler.logger.warn((Object)"Couldn't signal frontend when status changed for MetaObject with UUID: ".concat(((MetaInfo.StatusInfo)entryEvent.getValue()).getOID().toString()), (Throwable)e2);
            }
            catch (RMIWebSocketException e3) {
                MetaObjectStatusChangeWebNotificationHandler.logger.warn((Object)"Couldn't signal frontend when status changed for MetaObject with UUID: ".concat(((MetaInfo.StatusInfo)entryEvent.getValue()).getOID().toString()), (Throwable)e3);
            }
        }
    }
    
    static {
        MetaObjectStatusChangeWebNotificationHandler.logger = Logger.getLogger((Class)MDRepositoryCRUDWebNotificationHandler.class);
    }
}
