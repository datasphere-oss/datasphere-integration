package com.datasphere.source.lib.rollingpolicy.util;

import java.text.*;
import org.joda.time.format.*;
import org.joda.time.*;

public class RolloverFilenameFormat
{
    private final String filenamePrefix;
    private final String filenameSuffix;
    private long numericalSequence;
    private DateTimeFormatter dateTimeFormatter;
    private String currentFilename;
    private final String tokenValue;
    private DecimalFormat df;
    private final long fileLimit;
    private final int sequenceStart;
    private final int incrementSequenceBy;
    private boolean useDateTimeSequence;
    private boolean useEpochTime;
    private boolean useEpochTimeInMS;
    private static final String EPOCH_TIME = "epochtime";
    private static final String EPOCH_TIME_IN_MS = "epochtimems";
    private long currentSequenceNo;
    
    public RolloverFilenameFormat(final String pattern) {
        this(pattern, -1L, 0, 1, true);
    }
    
    public RolloverFilenameFormat(final String pattern, final long fileLimit, final int sequenceStart, final int incrementSequenceBy, final boolean addDefaultSequence) {
        this.numericalSequence = -1L;
        this.useDateTimeSequence = false;
        this.useEpochTime = false;
        this.useEpochTimeInMS = false;
        this.currentSequenceNo = -1L;
        String fileName = pattern;
        if (fileName.contains("%")) {
            final int count = this.countOccurrencesOfToken(fileName, '%');
            final int startIndexOfToken = fileName.indexOf(37);
            final int endIndexOfToken = fileName.lastIndexOf(37);
            String fileSuffix = "";
            String filePrefix = "";
            if (count != 2) {
                throw new IllegalArgumentException("Failure in initializing writer. Invalid filename " + fileName + " Valid filename tokens are %n...% and %DateTimeFormat%.");
            }
            final String token = fileName.substring(startIndexOfToken + 1, endIndexOfToken);
            if (startIndexOfToken == 0) {
                fileSuffix = fileName.substring(endIndexOfToken + 1);
            }
            else if (endIndexOfToken == fileName.length()) {
                filePrefix = fileName.substring(0, startIndexOfToken);
            }
            else {
                filePrefix = fileName.substring(0, startIndexOfToken);
                fileSuffix = fileName.substring(endIndexOfToken + 1, fileName.length());
            }
            this.tokenValue = token;
            this.filenamePrefix = filePrefix;
            this.filenameSuffix = fileSuffix;
        }
        else {
            String fileExtension = "";
            if (fileName.contains(".")) {
                final int index = fileName.indexOf(".");
                fileExtension = fileName.substring(index, fileName.length());
                fileName = fileName.substring(0, index);
            }
            this.filenamePrefix = fileName + ".";
            this.filenameSuffix = fileExtension;
            if (addDefaultSequence) {
                this.tokenValue = "nn";
            }
            else {
                this.tokenValue = null;
                this.currentFilename = fileName + fileExtension;
            }
        }
        this.fileLimit = fileLimit;
        this.sequenceStart = sequenceStart;
        this.incrementSequenceBy = incrementSequenceBy;
        if (this.tokenValue != null) {
            if (this.tokenValue.trim().isEmpty()) {
                throw new IllegalArgumentException("Specified token value for filename " + fileName + "is empty");
            }
            if (this.tokenValue.contains("n")) {
                final int countOfToken = this.countOccurrencesOfToken(this.tokenValue, 'n');
                String formatValue = "0";
                for (int i = 0; i < countOfToken - 1; ++i) {
                    formatValue += "0";
                }
                (this.df = (DecimalFormat)NumberFormat.getInstance()).applyPattern(formatValue);
                this.numericalSequence = sequenceStart;
            }
            else {
                if (this.tokenValue.equalsIgnoreCase("epochtime")) {
                    this.useEpochTime = true;
                }
                else if (this.tokenValue.equalsIgnoreCase("epochtimems")) {
                    this.useEpochTimeInMS = true;
                }
                else {
                    this.dateTimeFormatter = DateTimeFormat.forPattern(this.tokenValue);
                }
                this.useDateTimeSequence = true;
                this.numericalSequence = -1L;
            }
        }
    }
    
    public String getNextSequence() {
        if (this.numericalSequence != -1L) {
            this.currentSequenceNo = this.numericalSequence;
            this.currentFilename = this.filenamePrefix + this.df.format(this.numericalSequence) + this.filenameSuffix;
            this.numericalSequence += this.incrementSequenceBy;
            if (this.numericalSequence == this.fileLimit) {
                this.numericalSequence = this.sequenceStart;
            }
            return this.currentFilename;
        }
        if (this.useDateTimeSequence) {
            final DateTime currentTime = DateTime.now();
            if (this.useEpochTime) {
                final long epochTime = currentTime.getMillis() / 1000L;
                this.currentFilename = this.filenamePrefix + epochTime + this.filenameSuffix;
            }
            else if (this.useEpochTimeInMS) {
                this.currentFilename = this.filenamePrefix + currentTime.getMillis() + this.filenameSuffix;
            }
            else {
                this.currentFilename = this.filenamePrefix + currentTime.toString(this.dateTimeFormatter) + this.filenameSuffix;
            }
            return this.currentFilename;
        }
        return this.currentFilename;
    }
    
    public String getCurrentFileName() {
        return this.currentFilename;
    }
    
    public String getPreFileName() {
    		if(this.currentSequenceNo > 0) {
    			return this.filenamePrefix + this.df.format(this.currentSequenceNo - 1) + this.filenameSuffix;
    		}
    		return null;
    }
    
    private int countOccurrencesOfToken(final String identifier, final char token) {
        int count = 0;
        for (int i = 0; i < identifier.length(); ++i) {
            if (identifier.charAt(i) == token) {
                ++count;
            }
        }
        return count;
    }
    
    public long getCurrentSequenceNo() {
        return this.currentSequenceNo;
    }
    
    public static void main(final String[] args) throws Exception {
        final RolloverFilenameFormat filenameFormat = new RolloverFilenameFormat("/tmp/abc_%n%.log.gz", 2L, 0, 1, true);
        String s = filenameFormat.getNextSequence();
        System.out.println(s);
        s = filenameFormat.getNextSequence();
        System.out.println(s);
        s = filenameFormat.getNextSequence();
        System.out.println(s);
    }
}
