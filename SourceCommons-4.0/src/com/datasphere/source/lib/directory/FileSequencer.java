package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import java.io.*;

public abstract class FileSequencer implements WatcherCallback
{
    Logger logger;
    Map<String, Object> propMap;
    public static String LOG4J_SEQUENCER;
    public static String DEFAULT;
    protected int retryCnt;
    public static final int MAX_RETRY_CNT = 5;
    boolean flag;
    
    public FileSequencer(final Map<String, Object> map) throws IOException, AdapterException {
        this.logger = Logger.getLogger((Class)FileSequencer.class);
        this.flag = true;
        this.propMap = map;
    }
    
    public boolean isEmpty() {
        return true;
    }
    
    @Override
    public abstract void onFileCreate(final File p0);
    
    @Override
    public abstract void onFileDelete(final File p0);
    
    @Override
    public abstract void onFileModify(final File p0);
    
    public File getNextFile(final File file, final long position) {
        return null;
    }
    
    public long getPosition() {
        return 0L;
    }
    
    public boolean position(final String fileName) {
        return true;
    }
    
    protected void postProcess(final File file) {
    }
    
    protected void preProcess(final File file) {
    }
    
    protected int fileListSize() {
        return 0;
    }
    
    public boolean isEOFReached(final InputStream inputStream, final boolean fileChanged) throws IOException {
        if (fileChanged || inputStream.available() <= 0) {
            if (this.fileListSize() > 0) {
                if ((fileChanged || inputStream.available() <= 0) && this.retryCnt < 5) {
                    if (fileChanged || inputStream.available() <= 0) {
                        ++this.retryCnt;
                    }
                    else {
                        this.retryCnt = 0;
                    }
                    return false;
                }
                this.retryCnt = 0;
                return this.flag = true;
            }
            else {
                if (fileChanged) {
                    this.logger.debug((Object)"File has changed, so going to sequencer to get next file");
                    return true;
                }
                if (this.flag) {
                    this.logger.debug((Object)"No more file to process...");
                }
                this.flag = false;
            }
        }
        return false;
    }
    
    public static FileSequencer load(final Property tmpProp) throws AdapterException {
        FileSequencer sequencer = null;
        final String packageName = "com.datasphere.proc.";
        String className = tmpProp.getString(Property.ROLLOVER_STYLE, null);
        sequencer = load(className, tmpProp);
        if (sequencer == null) {
            if (className == null) {
                className = Property.DEFAULT_FILE_SEQUENCER;
            }
            try {
                String fullyQualifiedName;
                if (className.contains(".")) {
                    fullyQualifiedName = className;
                }
                else {
                    fullyQualifiedName = packageName + className;
                }
                Class<?> sequencerClass = null;
                try {
                    sequencerClass = Class.forName(fullyQualifiedName);
                }
                catch (Exception exp) {
                    try {
                        sequencerClass = Class.forName(className);
                    }
                    catch (Exception sExp) {
                        throw new AdapterException("Couldn't load {" + className + "} sequencer implementation");
                    }
                }
                sequencer = (FileSequencer)sequencerClass.getConstructor(Map.class).newInstance(tmpProp.getMap());
            }
            catch (Exception e) {
                throw new AdapterException("Got exception while instantiating FileSequencer {" + className + "}", (Throwable)e);
            }
        }
        return sequencer;
    }
    
    public static FileSequencer load(final String sequncerName, final Property tmpProp) throws AdapterException {
        FileSequencer sequencer = null;
        try {
            if (FileSequencer.LOG4J_SEQUENCER.equalsIgnoreCase(sequncerName)) {
                sequencer = new RollingFileSequencer(tmpProp.getMap());
            }
            else if (FileSequencer.DEFAULT.equalsIgnoreCase(sequncerName)) {
                sequencer = new DefaultFileSequencer(tmpProp.getMap());
            }
        }
        catch (IOException e) {
            throw new AdapterException("Got exception while instantiating FileSequencer {" + sequncerName + "}", (Throwable)e);
        }
        return sequencer;
    }
    
    static {
        FileSequencer.LOG4J_SEQUENCER = "log4j";
        FileSequencer.DEFAULT = "Default";
    }
    
    class FileDetail
    {
        File file;
        long position;
    }
}
