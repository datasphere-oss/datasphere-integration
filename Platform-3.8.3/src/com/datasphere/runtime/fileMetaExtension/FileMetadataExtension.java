package com.datasphere.runtime.fileMetaExtension;

import java.io.*;
import com.datasphere.uuid.*;

public class FileMetadataExtension implements Serializable
{
    private String parentComponent;
    private UUID parentComponentUUID;
    private String fileName;
    private String directoryName;
    private String status;
    private long creationTimeStamp;
    private byte[] lastEventCheckPointValue;
    private Long rollOverTimeStamp;
    private String reasonForRollOver;
    private Long sequenceNumber;
    private String field1;
    private String field2;
    private long wrapNumber;
    private long numberOfEvents;
    private String owner;
    private String firstEventPosition;
    private String lastEventPosition;
    private Long firstEventTimestamp;
    private Long lastEventTimestamp;
    private String distributionID;
    private long externalFileCreationTime;
    
    public String getParentComponent() {
        return this.parentComponent;
    }
    
    public void setParentComponent(final String parentComponent) {
        this.parentComponent = parentComponent;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }
    
    public String getDirectoryName() {
        return this.directoryName;
    }
    
    public void setDirectoryName(final String directoryName) {
        this.directoryName = directoryName;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    public void setStatus(final String status) {
        this.status = status;
    }
    
    public long getCreationTimeStamp() {
        return this.creationTimeStamp;
    }
    
    public void setCreationTimeStamp(final long creationTimeStamp) {
        this.creationTimeStamp = creationTimeStamp;
    }
    
    public byte[] getLastEventCheckPointValue() {
        return this.lastEventCheckPointValue;
    }
    
    public void setLastEventCheckPointValue(final byte[] lastEventCheckPointValue) {
        this.lastEventCheckPointValue = lastEventCheckPointValue;
    }
    
    public Long getSequenceNumber() {
        return this.sequenceNumber;
    }
    
    public void setSequenceNumber(final Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public String getField1() {
        return this.field1;
    }
    
    public void setField1(final String field1) {
        this.field1 = field1;
    }
    
    public String getField2() {
        return this.field2;
    }
    
    public void setField2(final String field2) {
        this.field2 = field2;
    }
    
    public Long getRollOverTimeStamp() {
        return this.rollOverTimeStamp;
    }
    
    public void setRollOverTimeStamp(final Long rollOverTimeStamp) {
        this.rollOverTimeStamp = rollOverTimeStamp;
    }
    
    public String getReasonForRollOver() {
        return this.reasonForRollOver;
    }
    
    public void setReasonForRollOver(final String reasonForRollOver) {
        this.reasonForRollOver = reasonForRollOver;
    }
    
    public UUID getParentComponentUUID() {
        return this.parentComponentUUID;
    }
    
    public void setParentComponentUUID(final UUID parentComponentUUID) {
        this.parentComponentUUID = parentComponentUUID;
    }
    
    public long getWrapNumber() {
        return this.wrapNumber;
    }
    
    public void setWrapNumber(final long wrapNumber) {
        this.wrapNumber = wrapNumber;
    }
    
    public long getNumberOfEvents() {
        return this.numberOfEvents;
    }
    
    public void setNumberOfEvents(final long numberOfEvents) {
        this.numberOfEvents = numberOfEvents;
    }
    
    public String getOwner() {
        return this.owner;
    }
    
    public void setOwner(final String owner) {
        this.owner = owner;
    }
    
    public String getFirstEventPosition() {
        return this.firstEventPosition;
    }
    
    public void setFirstEventPosition(final String firstEventPosition) {
        this.firstEventPosition = firstEventPosition;
    }
    
    public String getLastEventPosition() {
        return this.lastEventPosition;
    }
    
    public void setLastEventPosition(final String lastEventPosition) {
        this.lastEventPosition = lastEventPosition;
    }
    
    public Long getFirstEventTimestamp() {
        return this.firstEventTimestamp;
    }
    
    public void setFirstEventTimestamp(final Long firstEventTimestamp) {
        this.firstEventTimestamp = firstEventTimestamp;
    }
    
    public Long getLastEventTimestamp() {
        return this.lastEventTimestamp;
    }
    
    public void setLastEventTimestamp(final Long lastEventTimestamp) {
        this.lastEventTimestamp = lastEventTimestamp;
    }
    
    public long getExternalFileCreationTime() {
        return this.externalFileCreationTime;
    }
    
    public void setExternalFileCreationTime(final long externalFileCreationTime) {
        this.externalFileCreationTime = externalFileCreationTime;
    }
    
    public void setDistributionID(final String distributionID) {
        this.distributionID = distributionID;
    }
    
    public String getDistributionID() {
        return this.distributionID;
    }
    
    public enum Status
    {
        CREATED, 
        COMPLETED, 
        CRASHED, 
        PROCESSING;
    }
    
    public enum ReasonForRollOver
    {
        DDL, 
        ROLLOVER_EXPIRY, 
        EOF, 
        NONE;
    }
}
