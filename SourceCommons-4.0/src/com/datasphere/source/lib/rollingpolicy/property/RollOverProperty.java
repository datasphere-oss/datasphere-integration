package com.datasphere.source.lib.rollingpolicy.property;

import java.util.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.rollingpolicy.util.*;

public class RollOverProperty
{
    private String rollingPolicyName;
    private long fileSize;
    private long eventCount;
    private long timeInterval;
    private long fileLimit;
    private String filename;
    private boolean hasFormatterGotHeader;
    private int sequenceStart;
    private int incrementSequenceBy;
    private boolean append;
    private boolean addDefaultSequence;
    
    public RollOverProperty(final Map<String, Object> rollOverProperties) throws AdapterException {
        this.fileSize = 10485760L;
        this.eventCount = 100L;
        this.timeInterval = 43200000L;
        this.fileLimit = -1L;
        this.hasFormatterGotHeader = false;
        this.sequenceStart = 0;
        this.incrementSequenceBy = 1;
        this.append = false;
        this.addDefaultSequence = true;
        this.rollingPolicyName = (String)rollOverProperties.get("rolloverpolicy");
        if (this.rollingPolicyName.equalsIgnoreCase("DefaultRollingPolicy")) {
            this.append = true;
            this.addDefaultSequence = false;
        }
        final String fileSize = (String)rollOverProperties.get("filesize");
        if (fileSize != null) {
            this.fileSize = this.getFileSizeInBytes(fileSize);
        }
        final String eventCount = (String)rollOverProperties.get("eventcount");
        if (eventCount != null) {
            this.eventCount = Integer.parseInt(eventCount);
            if (this.eventCount < 0L) {
                this.eventCount = -this.eventCount;
            }
            else if (this.eventCount == 0L) {
                this.eventCount = 100L;
            }
        }
        final String timeInterval = (String)rollOverProperties.get("interval");
        if (timeInterval != null) {
            this.timeInterval = this.getRotationIntervalInMilliSeconds(timeInterval);
        }
        final String fileLimit = (String)rollOverProperties.get("filelimit");
        if (fileLimit != null) {
            this.fileLimit = Long.parseLong(fileLimit);
        }
        this.filename = (String)rollOverProperties.get("filename");
        final String formatterName = (String)rollOverProperties.get("handler");
        if (formatterName != null && (formatterName.equalsIgnoreCase("com.datasphere.proc.JSONFormatter") || formatterName.equalsIgnoreCase("com.datasphere.proc.XMLFormatter"))) {
            this.hasFormatterGotHeader = true;
        }
        final String sequenceStart = (String)rollOverProperties.get("sequencestart");
        if (sequenceStart != null) {
            this.sequenceStart = Integer.parseInt(sequenceStart);
        }
        final String incrementSequenceBy = (String)rollOverProperties.get("incrementsequenceby");
        if (incrementSequenceBy != null) {
            this.incrementSequenceBy = Integer.parseInt(incrementSequenceBy);
        }
    }
    
    public RollOverProperty(final String rolloverPropertyValue) throws AdapterException {
        this(RollingPolicyUtil.mapPolicyValueToRollOverPolicyName(rolloverPropertyValue));
    }
    
    public String getRollingPolicyName() {
        return this.rollingPolicyName;
    }
    
    public long getFileSize() {
        return this.fileSize;
    }
    
    public long getEventCount() {
        return this.eventCount;
    }
    
    public long getTimeInterval() {
        return this.timeInterval;
    }
    
    public long getFileLimit() {
        return this.fileLimit;
    }
    
    public String getFilename() {
        return this.filename;
    }
    
    public boolean hasFormatterGotHeader() {
        return this.hasFormatterGotHeader;
    }
    
    public int getSequenceStart() {
        return this.sequenceStart;
    }
    
    public int getIncrementSequenceBy() {
        return this.incrementSequenceBy;
    }
    
    public boolean shouldAppend() {
        return this.append;
    }
    
    public boolean shouldAddDefaultSequence() {
        return this.addDefaultSequence;
    }
    
    private long getFileSizeInBytes(String fileSize) throws AdapterException {
        fileSize = fileSize.toLowerCase();
        if (!fileSize.contains("m")) {
            throw new AdapterException("Please specify file size in Megabytes");
        }
        final int index = fileSize.indexOf("m");
        final String sizeInString = fileSize.substring(0, index);
        long size;
        try {
            size = Math.round(Double.parseDouble(sizeInString));
        }
        catch (NumberFormatException e) {
            throw new AdapterException("Specified file size " + fileSize + " is invalid", (Throwable)e);
        }
        if (size < 0L) {
            size = -size;
        }
        else if (size == 0L) {
            size = 10L;
        }
        return size * 1024L * 1024L;
    }
    
    private long getRotationIntervalInMilliSeconds(String rotationInterval) throws AdapterException {
        rotationInterval = rotationInterval.toLowerCase();
        final int length = rotationInterval.length();
        final String intervalString = rotationInterval.substring(0, length - 1);
        long interval;
        try {
            interval = Math.round(Double.parseDouble(intervalString));
        }
        catch (NumberFormatException e) {
            throw new AdapterException("Specified rotation interval " + rotationInterval + " is invalid", (Throwable)e);
        }
        if (interval < 0L) {
            interval = -interval;
        }
        else if (interval == 0L) {
            interval = 12L;
        }
        final String timeGranularity = rotationInterval.substring(length - 1);
        if (timeGranularity.contains("s")) {
            return interval * 1000L;
        }
        if (timeGranularity.contains("m")) {
            return interval * 60L * 1000L;
        }
        if (timeGranularity.contains("h")) {
            return interval * 60L * 60L * 1000L;
        }
        if (timeGranularity.contains("d")) {
            return interval * 24L * 60L * 60L * 1000L;
        }
        throw new AdapterException("Specified rotation interval " + rotationInterval + " is invalid");
    }
}
