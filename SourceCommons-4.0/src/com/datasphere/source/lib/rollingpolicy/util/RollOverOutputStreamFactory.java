package com.datasphere.source.lib.rollingpolicy.util;

import com.datasphere.source.lib.rollingpolicy.outputstream.*;
import com.datasphere.source.lib.rollingpolicy.property.*;

import java.io.*;

public class RollOverOutputStreamFactory
{
    public static RollOverOutputStream getRollOverOutputStream(final RollOverProperty rollOverProperty, final RollOverOutputStream.OutputStreamBuilder outputStreamBuilder, final RolloverFilenameFormat filenameFormat) throws IOException {
        if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("EventCountRollingPolicy")) {
            return new EventCountRollOverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getEventCount(), rollOverProperty.hasFormatterGotHeader());
        }
        if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("FileSizeRollingPolicy")) {
            return new FilelengthRolloverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getFileSize());
        }
        if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("TimeIntervalRollingPolicy")) {
            return new TimeIntervalRollOverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getTimeInterval(), rollOverProperty.hasFormatterGotHeader());
        }
        if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("EventCountAndTimeRollingPolicy")) {
            return new EventCountAndTimeRolloverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getEventCount(), rollOverProperty.getTimeInterval(), rollOverProperty.hasFormatterGotHeader());
        }
        return new RollOverOutputStream(outputStreamBuilder, filenameFormat) {};
    }
}
