package com.datasphere.source.lib.rollingpolicy.util;

public class DynamicRollOverFilenameFormat extends RolloverFilenameFormat
{
    private String currentWorkingDirectoryName;
    
    public DynamicRollOverFilenameFormat(final String pattern, final long fileLimit, final int startSequence, final int incrementSequenceBy, final boolean addDefaultSequence, final String currentWorkingDirectoryName) {
        super(pattern, fileLimit, startSequence, incrementSequenceBy, addDefaultSequence);
        this.currentWorkingDirectoryName = currentWorkingDirectoryName;
    }
    
    @Override
    public String getNextSequence() {
        return this.currentWorkingDirectoryName + super.getNextSequence();
    }
}
