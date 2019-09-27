package com.datasphere.source.lib.utils;

import org.apache.log4j.*;
import com.datasphere.common.exc.*;

public class NameAndCreationTime extends DefaultFileComparator
{
    private Logger logger;
    
    public NameAndCreationTime() {
        this.logger = Logger.getLogger((Class)NameAndCreationTime.class);
    }
    
    @Override
    public int compare(final Object o1, final Object o2) {
        long diff = 0L;
        try {
            diff = this.getFileName(o1).compareTo(this.getFileName(o2));
            if (diff == 0L) {
                diff = this.getFileCreationTime(o1) - this.getFileCreationTime(o2);
            }
        }
        catch (AdapterException e) {
            this.logger.error((Object)e.getErrorMessage());
        }
        return (int)diff;
    }
}
