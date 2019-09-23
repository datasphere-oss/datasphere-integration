package com.datasphere.recovery;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;

public abstract class AbstractCheckpointPath
{
    private static final long serialVersionUID = -1850611944670314876L;
    private static Logger logger;
    public int id;
    public String idStr;
    public final String flowUuid;
    public Path.ItemList pathItems;
    public SourcePosition lowSourcePosition;
    public SourcePosition highSourcePosition;
    public boolean atOrAfter;
    public Timestamp updated;
    
    protected AbstractCheckpointPath() {
        this.id = 0;
        this.idStr = null;
        this.flowUuid = null;
        this.pathItems = null;
        this.lowSourcePosition = null;
        this.highSourcePosition = null;
    }
    
    public AbstractCheckpointPath(final UUID flowUuid, final Path.ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final String atOrAfter) {
        this.id = 0;
        this.idStr = null;
        this.flowUuid = ((flowUuid == null) ? null : flowUuid.getUUIDString());
        this.pathItems = pathItems;
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = highSourcePosition;
        this.atOrAfter = "^".equals(atOrAfter);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof CheckpointPath)) {
            return false;
        }
        final CheckpointPath that = (CheckpointPath)obj;
        return this.flowUuid == null == (that.flowUuid == null) && this.flowUuid.equals(that.flowUuid) && this.pathItems == null == (that.pathItems == null) && this.pathItems.equals(that.pathItems) && this.lowSourcePosition == null == (that.lowSourcePosition == null) && this.lowSourcePosition.compareTo(that.lowSourcePosition) == 0 && this.highSourcePosition == null == (that.highSourcePosition == null) && this.highSourcePosition.compareTo(that.highSourcePosition) == 0 && this.atOrAfter == that.atOrAfter && this.highSourcePosition.compareTo(that.highSourcePosition) == 0;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.toString()).toHashCode();
    }
    
    @Override
    public String toString() {
        return "[" + this.flowUuid + ", " + this.pathItems + ", " + this.lowSourcePosition + "]";
    }
    
    public String getIdString() {
        if (this.idStr == null) {
            this.idStr = getHash(this.pathItems);
        }
        return this.idStr;
    }
    
    public void setIdString(final String id) {
        this.idStr = id;
    }
    
    public String getPathItemsAsString() {
        return this.pathItems.toString();
    }
    
    public void setPathItemsAsString(final String st) {
        this.pathItems = Path.ItemList.fromString(st);
    }
    
    public static String getHash(final Path.ItemList items) {
        String retStr = null;
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final byte[] digest = md.digest(items.getBytes());
            retStr = DatatypeConverter.printHexBinary(digest);
        }
        catch (NoSuchAlgorithmException e) {
            AbstractCheckpointPath.logger.error((Object)"Unable to store checkpoint");
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
            AbstractCheckpointPath.logger.error((Object)"Unable to store checkpoint");
        }
        return retStr;
    }
    
    static {
        AbstractCheckpointPath.logger = Logger.getLogger((Class)CheckpointPath.class);
    }
}
