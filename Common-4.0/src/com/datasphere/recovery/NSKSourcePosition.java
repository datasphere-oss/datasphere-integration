package com.datasphere.recovery;

import java.util.concurrent.*;
import java.util.*;

public class NSKSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -6042045728463138984L;
    private Map<String, String> positionMap;
    private String currentAuditTrailName;
    
    public NSKSourcePosition() {
        this.currentAuditTrailName = null;
        this.positionMap = new ConcurrentHashMap<String, String>();
    }
    
    public NSKSourcePosition(final NSKSourcePosition sp) {
        (this.positionMap = new ConcurrentHashMap<String, String>()).putAll(sp.positionMap);
        this.currentAuditTrailName = sp.currentAuditTrailName;
    }
    
    public synchronized void updateAuditTrail(final String auditTrailName, final String position) {
        this.currentAuditTrailName = auditTrailName;
        if (this.positionMap.containsKey(this.currentAuditTrailName)) {
            final String[] oldPos = this.positionMap.get(this.currentAuditTrailName).split(":");
            final String[] newPos = position.split(":");
            final int compare = Long.valueOf(newPos[0]).compareTo(Long.valueOf(oldPos[0]));
            if (compare > 0) {
                this.positionMap.put(this.currentAuditTrailName, position);
            }
        }
        else {
            this.positionMap.put(this.currentAuditTrailName, position);
        }
    }
    
    public String getCurrentAuditTrailName() {
        return this.currentAuditTrailName;
    }
    
    public ConcurrentHashMap<String, String> getPositionMap() {
        return (ConcurrentHashMap<String, String>)(ConcurrentHashMap)this.positionMap;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final String atName = this.currentAuditTrailName;
        if (this.positionMap.containsKey(atName) && ((NSKSourcePosition)arg0).positionMap.containsKey(atName)) {
            final String[] thisPos = this.positionMap.get(atName).split(":");
            final String[] thatPos = ((NSKSourcePosition)arg0).positionMap.get(atName).split(":");
            final int compare = Long.valueOf(thisPos[0]).compareTo(Long.valueOf(thatPos[0]));
            return compare;
        }
        return 1;
    }
    
    @Override
    public String toString() {
        String position = "";
        if (!this.positionMap.isEmpty()) {
            for (final Map.Entry<String, String> entry : this.positionMap.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();
                position = position + key + "-" + value + ";";
            }
        }
        return position;
    }
}
