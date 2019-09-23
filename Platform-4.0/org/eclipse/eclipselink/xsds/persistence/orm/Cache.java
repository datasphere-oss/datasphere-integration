package org.eclipse.eclipselink.xsds.persistence.orm;

import java.math.*;
import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache", propOrder = { "expiry", "expiryTimeOfDay" })
public class Cache
{
    protected BigInteger expiry;
    @XmlElement(name = "expiry-time-of-day")
    protected TimeOfDay expiryTimeOfDay;
    @XmlAttribute(name = "size")
    protected BigInteger size;
    @XmlAttribute(name = "shared")
    protected Boolean shared;
    @XmlAttribute(name = "isolation")
    protected CacheIsolationType isolation;
    @XmlAttribute(name = "type")
    protected CacheType type;
    @XmlAttribute(name = "always-refresh")
    protected Boolean alwaysRefresh;
    @XmlAttribute(name = "refresh-only-if-newer")
    protected Boolean refreshOnlyIfNewer;
    @XmlAttribute(name = "disable-hits")
    protected Boolean disableHits;
    @XmlAttribute(name = "coordination-type")
    protected CacheCoordinationType coordinationType;
    @XmlAttribute(name = "database-change-notification-type")
    protected DatabaseChangeNotificationType databaseChangeNotificationType;
    
    public BigInteger getExpiry() {
        return this.expiry;
    }
    
    public void setExpiry(final BigInteger value) {
        this.expiry = value;
    }
    
    public TimeOfDay getExpiryTimeOfDay() {
        return this.expiryTimeOfDay;
    }
    
    public void setExpiryTimeOfDay(final TimeOfDay value) {
        this.expiryTimeOfDay = value;
    }
    
    public BigInteger getSize() {
        return this.size;
    }
    
    public void setSize(final BigInteger value) {
        this.size = value;
    }
    
    public Boolean isShared() {
        return this.shared;
    }
    
    public void setShared(final Boolean value) {
        this.shared = value;
    }
    
    public CacheIsolationType getIsolation() {
        return this.isolation;
    }
    
    public void setIsolation(final CacheIsolationType value) {
        this.isolation = value;
    }
    
    public CacheType getType() {
        return this.type;
    }
    
    public void setType(final CacheType value) {
        this.type = value;
    }
    
    public Boolean isAlwaysRefresh() {
        return this.alwaysRefresh;
    }
    
    public void setAlwaysRefresh(final Boolean value) {
        this.alwaysRefresh = value;
    }
    
    public Boolean isRefreshOnlyIfNewer() {
        return this.refreshOnlyIfNewer;
    }
    
    public void setRefreshOnlyIfNewer(final Boolean value) {
        this.refreshOnlyIfNewer = value;
    }
    
    public Boolean isDisableHits() {
        return this.disableHits;
    }
    
    public void setDisableHits(final Boolean value) {
        this.disableHits = value;
    }
    
    public CacheCoordinationType getCoordinationType() {
        return this.coordinationType;
    }
    
    public void setCoordinationType(final CacheCoordinationType value) {
        this.coordinationType = value;
    }
    
    public DatabaseChangeNotificationType getDatabaseChangeNotificationType() {
        return this.databaseChangeNotificationType;
    }
    
    public void setDatabaseChangeNotificationType(final DatabaseChangeNotificationType value) {
        this.databaseChangeNotificationType = value;
    }
}
