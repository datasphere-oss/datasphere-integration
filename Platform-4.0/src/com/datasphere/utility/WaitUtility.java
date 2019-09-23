package com.datasphere.utility;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.Event;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class WaitUtility implements Serializable
{
    private final Map<Event.EventAction, Map<Long, Set<UUID>>> waitingDetails;
    
    public WaitUtility() {
        this.waitingDetails = new HashMap<Event.EventAction, Map<Long, Set<UUID>>>();
    }
    
    public void startWaiting(final Event.EventAction commandType, final Long commandTimestamp, final Set<UUID> waitSet) {
        Map<Long, Set<UUID>> timestampToUuids = this.waitingDetails.get(commandType);
        if (timestampToUuids == null) {
            timestampToUuids = new HashMap<Long, Set<UUID>>();
            this.waitingDetails.put(commandType, timestampToUuids);
        }
        assert !timestampToUuids.containsKey(commandTimestamp);
        timestampToUuids.put(commandTimestamp, waitSet);
        if (!waitSet.isEmpty() && Logger.getLogger("Commands").isTraceEnabled()) {
            Logger.getLogger("Commands").trace((Object)("START WAITING on " + waitSet.size() + " components to " + commandType + " at timestamp " + commandTimestamp));
            for (final UUID u : waitSet) {
                try {
                    final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(u, HSecurityManager.TOKEN);
                    Logger.getLogger("Commands").trace((Object)("       * " + ((mo == null) ? "not found" : mo.getName())));
                }
                catch (MetaDataRepositoryException e) {
                    Logger.getLogger("Commands").error((Object)("       * (error) " + u));
                }
            }
        }
    }
    
    public boolean isWaitingOn(final Event.EventAction commandType, final Long commandTimestamp, final UUID waitingOnThis) {
        if (!this.waitingDetails.containsKey(commandType)) {
            return false;
        }
        final Map<Long, Set<UUID>> timestampToUuids = this.waitingDetails.get(commandType);
        if (!timestampToUuids.containsKey(commandTimestamp)) {
            return false;
        }
        final Set<UUID> waitSet = timestampToUuids.get(commandTimestamp);
        return waitSet.contains(waitingOnThis);
    }
    
    public boolean isWaitingOn(final Long commandTimestamp) {
        for (final Map<Long, Set<UUID>> waitMaps : this.waitingDetails.values()) {
            if (waitMaps.containsKey(commandTimestamp)) {
                return true;
            }
        }
        return false;
    }
    
    public int stopWaitingOn(final Event.EventAction commandType, final long commandTimestamp, final UUID compUuid) {
        if (!this.waitingDetails.containsKey(commandType)) {
            throw new RuntimeException("Cannot stop waiting on " + commandType + " at " + commandTimestamp + " for " + compUuid + " because this node isn't waiting on that command!");
        }
        final Map<Long, Set<UUID>> timestampToUuids = this.waitingDetails.get(commandType);
        if (!timestampToUuids.containsKey(commandTimestamp)) {
            throw new RuntimeException("Cannot stop waiting on " + commandType + " at " + commandTimestamp + " for " + compUuid + " because this node isn't waiting on that timestamp!");
        }
        final Set<UUID> waitSet = timestampToUuids.get(commandTimestamp);
        waitSet.remove(compUuid);
        if (waitSet.isEmpty()) {
            timestampToUuids.remove(commandTimestamp);
        }
        if (!waitSet.isEmpty() && Logger.getLogger("Commands").isTraceEnabled()) {
            Logger.getLogger("Commands").trace((Object)("Stop waiting on " + compUuid + " of type " + commandType + " at timestamp " + commandTimestamp));
            Logger.getLogger("Commands").trace((Object)("     ...STILL WAITING on " + waitSet.size() + " components to " + commandType));
            for (final UUID u : waitSet) {
                try {
                    final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(u, HSecurityManager.TOKEN);
                    Logger.getLogger("Commands").trace((Object)("       * " + ((mo == null) ? "not found" : mo.getName())));
                }
                catch (MetaDataRepositoryException e) {
                    Logger.getLogger("Commands").error((Object)("       * (error) " + u));
                }
            }
        }
        final int result = waitSet.size();
        return result;
    }
    
    public void clear() {
        if (Logger.getLogger("Commands").isInfoEnabled() && !this.waitingDetails.isEmpty()) {
            Logger.getLogger("Commands").info((Object)"Clearing non-empty wait details (this is normal after a crash):");
            for (final Event.EventAction commandType : this.waitingDetails.keySet()) {
                Logger.getLogger("Commands").info((Object)("Type: " + commandType));
                final Map<Long, Set<UUID>> timestampToUuids = this.waitingDetails.get(commandType);
                for (final Long commandTimestamp : timestampToUuids.keySet()) {
                    Logger.getLogger("Commands").info((Object)("Timestamp: " + commandTimestamp));
                    final Set<UUID> waitSet = timestampToUuids.get(commandTimestamp);
                    for (final UUID uuid : waitSet) {
                        Logger.getLogger("Commands").info((Object)("UUID: " + uuid));
                    }
                }
            }
        }
        this.waitingDetails.clear();
    }
}
