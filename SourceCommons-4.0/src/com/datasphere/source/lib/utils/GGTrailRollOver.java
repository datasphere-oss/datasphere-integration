package com.datasphere.source.lib.utils;

import java.io.*;

public class GGTrailRollOver extends DefaultFileComparator
{
    @Override
    public int compare(final Object o1, final Object o2) {
        final File f1 = (File)o1;
        final File f2 = (File)o2;
        long diff = this.extractSeqNumber(f1.getName()) - this.extractSeqNumber(f2.getName());
        if (diff == 0L) {
            diff = this.getFileCreationTime(f1) - this.getFileCreationTime(f2);
        }
        return (int)diff;
    }
    
    private int extractSeqNumber(final String name) {
        int len;
        for (len = name.length(); len > 0 && Character.isDigit(name.charAt(len - 1)); --len) {}
        final String sequence = name.substring(len);
        return Integer.parseInt(sequence);
    }
}
