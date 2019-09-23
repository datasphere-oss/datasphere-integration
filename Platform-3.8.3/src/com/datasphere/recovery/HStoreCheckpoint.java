package com.datasphere.recovery;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

public class HStoreCheckpoint implements Serializable
{
    private static final long serialVersionUID = -1850611944670314876L;
    private static Logger logger;
    public String id;
    public Path.ItemList pathItems;
    public SourcePosition sourcePosition;
    public Timestamp updated;
    
    private HStoreCheckpoint() {
        this.id = null;
        this.pathItems = null;
        this.sourcePosition = null;
    }
    
    public static String getHash(final Path.ItemList items) {
        String retStr = null;
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final byte[] digest = md.digest(items.getBytes());
            retStr = DatatypeConverter.printHexBinary(digest);
        }
        catch (NoSuchAlgorithmException e) {
            HStoreCheckpoint.logger.error((Object)"Unable to store checkpoint");
        }
        return retStr;
    }
    
    public static String getHash(final String hdStoreName, final Long checkpointSequence, final Path.ItemList items) {
        String retStr = null;
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(hdStoreName.getBytes());
            md.update(checkpointSequence.toString().getBytes());
            md.update(items.getBytes());
            final byte[] digest = md.digest();
            retStr = DatatypeConverter.printHexBinary(digest);
        }
        catch (NoSuchAlgorithmException e) {
            HStoreCheckpoint.logger.error((Object)"Unable to store checkpoint");
        }
        return retStr;
    }
    
    public HStoreCheckpoint(final Path.ItemList pathUuids, final SourcePosition sourcePosition) {
        this.id = null;
        this.pathItems = pathUuids;
        this.sourcePosition = sourcePosition;
    }
    
    public String getIdString() {
        if (this.id == null) {
            this.id = getHash(this.pathItems);
        }
        return this.id;
    }
    
    public void setIdString(final String id) {
        this.id = id;
    }
    
    public String getPathItemsAsString() {
        return this.pathItems.toString();
    }
    
    public void setPathItemsAsString(final String st) {
        this.pathItems = Path.ItemList.fromString(st);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof HStoreCheckpoint)) {
            return false;
        }
        final HStoreCheckpoint that = (HStoreCheckpoint)obj;
        if (this.pathItems.size() != that.pathItems.size()) {
            return false;
        }
        for (int i = 0; i < this.pathItems.size(); ++i) {
            if (!this.pathItems.get(i).equals((Object)that.pathItems.get(i))) {
                return false;
            }
        }
        return this.sourcePosition.compareTo(that.sourcePosition) == 0;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.toString()).toHashCode();
    }
    
    @Override
    public String toString() {
        return "[" + this.pathItems.toString() + ", " + this.sourcePosition + "]";
    }
    
    public void prePersist() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    public void preUpdate() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    static {
        HStoreCheckpoint.logger = Logger.getLogger((Class)HStoreCheckpoint.class);
    }
}
