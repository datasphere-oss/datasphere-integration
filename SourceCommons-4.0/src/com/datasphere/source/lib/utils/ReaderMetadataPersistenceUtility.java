package com.datasphere.source.lib.utils;

import com.datasphere.runtime.fileMetaExtension.*;
import com.datasphere.source.lib.reader.*;

public class ReaderMetadataPersistenceUtility
{
    public static void persistEventCountAndLastEventTimestamp(final Reader reader, final long eventCount, final long lastEventTimestamp) {
        final FileMetadataExtension fme = reader.getFileMetadataExtension();
        if (fme != null) {
            fme.setNumberOfEvents(eventCount);
            fme.setLastEventTimestamp(lastEventTimestamp);
        }
    }
    
    public static void persistFirstEventTimestamp(final Reader reader, final long firstEventTimeStamp) {
        final FileMetadataExtension fme = reader.getFileMetadataExtension();
        if (fme != null) {
            fme.setFirstEventTimestamp(firstEventTimeStamp);
        }
    }
}
