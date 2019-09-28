package com.datasphere.io.hdfs.reader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.directory.Directory;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.FileReader;
import com.datasphere.io.hdfs.commmon.HDFSCommon;
import com.datasphere.io.hdfs.directory.HDFSDirectory;

public class HDFSReader extends FileReader
{
    String hadoopUri;
    FileSystem hadoopFileSystem;
    Logger logger;
    private HDFSCommon hdfsCommon;
    Directory dir;
    public static final int MAX_RETRY_CNT = 5;
    
    public HDFSReader(final Property prop) throws AdapterException {
        super(prop);
        this.logger = Logger.getLogger((Class)HDFSReader.class);
        this.prop = prop;
        this.hdfsCommon = new HDFSCommon(prop);
    }
    
    public void init() throws AdapterException {
        this.hadoopFileSystem = this.hdfsCommon.getHDFSInstance();
        super.init();
        this.hdfsinit();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("HDFSReader is initialized with following properties\nHadoopURL - [" + this.prop.hadoopUrl + "]\nWildcard - [" + this.prop.wildcard + "]\nEOFDelay - [" + this.prop.eofdelay + "]\nPositionByEOF - [" + this.prop.positionByEOF + "]\nRollOverPolicy - [" + this.prop.rollOverPolicy + "]"));
        }
    }
    
    private void hdfsinit() throws AdapterException {
        (this.buffer = ByteBuffer.allocate(this.blockSize())).clear();
        this.positionByEOF = this.prop.positionByEOF;
        this.skipBOM = this.prop.skipBOM;
        this.breakOnNoRecord = this.prop.getBoolean("breakonnorecord", false);
        this.dir = this.createDirectoryInstance();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("FileReader is initialized with following properties\nDirectory - [" + this.prop.directory + "]\nWildcard - [" + this.prop.wildcard + "]\nBlocksize - [" + this.prop.blocksize + "]\nPositionByEOF - [" + this.prop.positionByEOF + "]\nRollOverPolicy - [" + this.prop.rollOverPolicy + "]"));
        }
    }
    
    private Directory createDirectoryInstance() throws AdapterException {
        Comparator<Object> policy = null;
        try {
            final Class<?> obj = Class.forName("com.datasphere.io.hdfs.comparator.HDFSComparator");
            policy = (Comparator<Object>)obj.getConstructor(FileSystem.class).newInstance(this.hadoopFileSystem);
            return new HDFSDirectory(this.prop, this.hadoopFileSystem, policy);
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | IOException | InterruptedException e) {
            final AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            throw se;
        }
    }
    
    protected boolean openFile() throws AdapterException {
        if (this.dir.getFileListSize() > 0) {
            if (this.recovery) {
                this.currentFile = this.dir.getFile(this.fileToBeRecovered);
                if (this.currentFile == null) {
                    return false;
                }
            }
            else if (this.positionByEOF) {
                while (!this.dir.isLastFile()) {
                    this.currentFile = this.dir.getNextFile();
                }
            }
            else {
                this.currentFile = this.dir.getNextFile();
            }
            return this.open(this.currentFile);
        }
        return false;
    }
    
    protected boolean open(final Object file) throws AdapterException {
        final Path fileToBeOpened = (Path)file;
        final String hadoopUrl = this.hdfsCommon.getHadoopUrl();
        this.name(this.hadoopUri = hadoopUrl + fileToBeOpened.getName());
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("File [" + fileToBeOpened.getName() + "] opened from HDFS directory [" + hadoopUrl + "]"));
        }
        try {
            this.eventMetadataMap.put("FileName", this.hadoopUri);
            if (this.positionByEOF) {
                this.eofPosition = this.hadoopFileSystem.getFileStatus(fileToBeOpened).getLen();
                this.eventMetadataMap.put("FileOffset", this.eofPosition);
            }
            else {
                this.eventMetadataMap.put("FileOffset", 0);
            }
            this.recoveryCheckpoint.setSourceName(this.hadoopUri);
            this.creationTime = this.hadoopFileSystem.getFileStatus(fileToBeOpened).getModificationTime();
            this.recoveryCheckpoint.setSourceCreationTime(this.creationTime);
            this.inputStream = this.hadoopFileSystem.open(new Path(this.hadoopUri));
            this.createOrUpdateFileMetadataEntry(fileToBeOpened.getName(), hadoopUrl);
            this.currentFileName = " " + this.hadoopUri;
            this.onOpen((Object)this.hadoopUri);
            this.setChanged();
            this.notifyObservers((Object)Constant.eventType.ON_OPEN);
        }
        catch (IllegalArgumentException | IOException e) {
            final AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            throw se;
        }
        return true;
    }
    
    protected boolean isEOFReached() throws AdapterException {
        try {
            if (this.isEOFReached(this.inputStream, this.fileChanged) && this.openFile()) {
                return true;
            }
        }
        catch (IOException e) {
            final String eMsg = "Got exception while checking for EOF {" + ((File)this.currentFile).getName() + "}";
            if (this.breakOnNoRecord) {
                throw new RuntimeException(eMsg, (Throwable)new RecordException(eMsg, RecordException.Type.END_OF_DATASOURCE));
            }
            throw new AdapterException(eMsg, (Throwable)e);
        }
        return false;
    }
    
    private boolean isEOFReached(final InputStream inputStream, final boolean fileChanged) throws IOException {
        if ((!fileChanged && inputStream.available() > 0) || this.dir.getFileListSize() <= 0) {
            return false;
        }
        if ((fileChanged || inputStream.available() <= 0) && this.retryCnt < 5) {
            if (fileChanged || inputStream.available() <= 0) {
                ++this.retryCnt;
            }
            else {
                this.retryCnt = 0;
            }
            return false;
        }
        this.onClose((Object)this.name());
        this.inputStream.close();
        this.updateFileMetadataEntry();
        this.inputStream = null;
        this.printCloseTrace();
        this.retryCnt = 0;
        this.lastRolledOverFile = " " + this.hadoopUri;
        this.lastRolledOverTime = " " + new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").format(new Date());
        return true;
    }
    
    public void close() throws IOException {
        super.close();
        if (this.dir != null) {
            this.dir.stopThread();
        }
        if (this.hadoopFileSystem != null) {
            this.hadoopFileSystem.close();
        }
    }
    
    public Map<String, Object> getEventMetadata() {
        return (Map<String, Object>)this.eventMetadataMap;
    }
    
    public long skipBytes(final long offset) throws AdapterException {
        if (this.inputStream != null) {
            try {
                ((FSDataInputStream)this.inputStream).seek(offset);
            }
            catch (IOException e) {
                final String errMsg = "Got IOException while positioning HDFS stream {" + this.name() + "} offset {" + offset + "}";
                throw new AdapterException(errMsg, (Throwable)e);
            }
        }
        return offset;
    }
    
    public void position(final CheckpointDetail record, final boolean position) throws AdapterException {
        if (record != null) {
            record.dump();
            this.recovery = true;
            this.recoveryCheckpoint.setRecovery(this.recovery);
            if (record.getRecordEndOffset() != null) {
                this.recoveryCheckpoint.setRecordEndOffset((long)record.getRecordBeginOffset());
            }
            this.fileToBeRecovered = record.getSourceName();
            try {
                if (!this.fileToBeRecovered.equals(this.name()) && !this.openFile()) {
                    final String errMsg = "Trying to recovery from file {" + this.name() + "} which is not found";
                    this.logger.warn((Object)errMsg);
                    final AdapterException se = new AdapterException(errMsg);
                    throw se;
                }
            }
            catch (AdapterException e) {
                throw e;
            }
            if (position) {
                this.validateSource(record);
                this.skipBytes(record.seekPosition());
                this.recoveryCheckpoint.seekPosition((long)record.seekPosition());
            }
        }
        else {
            this.recovery = false;
            this.recoveryCheckpoint.setRecovery(this.recovery);
            try {
                if (!this.openFile()) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)"Input directory is empty");
                    }
                    this.startFromBegining = true;
                }
                else {
                    if (this.positionByEOF && position && !this.startFromBegining) {
                        this.skipBytes(this.eofPosition);
                        this.recoveryCheckpoint.seekPosition(this.eofPosition);
                    }
                    else {
                        this.recoveryCheckpoint.seekPosition(0L);
                    }
                    this.startFromBegining = false;
                }
            }
            catch (AdapterException e) {
                throw e;
            }
        }
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
