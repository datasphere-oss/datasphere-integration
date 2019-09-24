package com.datasphere.kafka;

import java.io.*;
import java.util.*;
/*
 * 检查点信息
 */
public class KWCheckpoint implements Serializable
{
    private static final long serialVersionUID = -1850611944670314876L;
    public String checkpointkey;
    public byte[] checkpointdata;
    
    public KWCheckpoint() {
    }
    
    public KWCheckpoint(final String key, final byte[] checkpointdata) {
        this.checkpointkey = key;
        this.checkpointdata = checkpointdata;
    }
    
    @Override
    public String toString() {
        return this.checkpointkey + ":" + Arrays.toString(this.checkpointdata);
    }
}
