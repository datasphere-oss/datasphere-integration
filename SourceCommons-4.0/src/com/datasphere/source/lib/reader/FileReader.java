package com.datasphere.source.lib.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.metaRepository.FileMetadataExtensionRepository;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.meta.cdc.filters.FileMetadataFilter;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.recovery.Path;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.directory.FileBank;
import com.datasphere.source.lib.prop.Property;

public class FileReader extends ReaderBase
{
    protected FileBank fb;
    protected Object currentFile;
    protected ByteBuffer buffer;
    protected int retryCnt;
    private Logger logger;
    protected long eofPosition;
    protected long creationTime;
    protected boolean positionByEOF;
    protected boolean recovery;
    protected boolean skipBOM;
    protected String fileToBeRecovered;
    protected boolean startFromBegining;
    protected long bytesRead;
    protected long readByteAddress;
    protected long previousBytesRead;
    protected Property prop;
    protected boolean isFirstTime;
    boolean closeCalled;
    protected long bytesConsumed;
    protected String fileKey;
    protected boolean fileChanged;
    private boolean readFromSubDirectories;
    protected String lastRolledOverFile;
    protected String lastRolledOverTime;
    protected String currentFileName;
    private boolean eofStatus;
    private FileMetadataExtensionRepository fileMetadataExtensionRepository;
    private FileMetadataExtension fileMetadataExtension;
    private boolean enableFLM;
    protected boolean deleteAfterUse;
    Long prevBytesConsumed;
    
    public FileReader(final Property prop) throws AdapterException {
        super(prop);
        this.retryCnt = 0;
        this.logger = Logger.getLogger((Class)FileReader.class);
        this.eofPosition = 0L;
        this.positionByEOF = true;
        this.recovery = false;
        this.bytesRead = 0L;
        this.readByteAddress = 0L;
        this.previousBytesRead = 0L;
        this.isFirstTime = true;
        this.fileKey = "";
        this.fileChanged = false;
        this.readFromSubDirectories = false;
        this.lastRolledOverFile = " N/A";
        this.lastRolledOverTime = " N/A";
        this.currentFileName = " N/A";
        this.eofStatus = true;
        this.fileMetadataExtensionRepository = FileMetadataExtensionRepository.getInstance();
        this.fileMetadataExtension = null;
        this.prevBytesConsumed = 0L;
        this.prop = prop;
    }
    
    public void init() throws AdapterException {
        super.init();
        (this.buffer = ByteBuffer.allocate(this.blockSize())).clear();
        this.positionByEOF = this.prop.positionByEOF;
        this.skipBOM = this.prop.skipBOM;
        this.breakOnNoRecord = this.prop.getBoolean("breakonnorecord", false);
        this.deleteAfterUse = this.prop.getBoolean(Reader.DELETE_AFTER_USE, false);
        final String adapterName = this.prop.getString("adapterName", "");
        if (!adapterName.contains("HDFS") && !adapterName.contains("MapRFS") && !adapterName.contains("Azure")) {
            this.fb = (FileBank)this.prop.getObject(FileBank.INSTANCE, null);
            if (this.fb == null) {
                (this.fb = new FileBank(this.prop)).start();
            }
            else {
                this.fb.updateInstance(this.prop);
            }
        }
        this.readFromSubDirectories = this.prop.getBoolean("includesubdirectories", false);
        this.enableFLM = this.prop.getBoolean("enableFLM", true);
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("FileReader is initialized with following properties\nDirectory - [" + this.prop.directory + "]\nWildcard - [" + this.prop.wildcard + "]\nBlocksize - [" + this.prop.blocksize + "]\nPositionByEOF - [" + this.prop.positionByEOF + "]\nRollOverPolicy - [" + this.prop.rollOverPolicy + "]"));
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        int bytesRead = 0;
        if (this.inputStream == null) {
            this.position(null, false);
            return null;
        }
        while (true) {
            try {
                this.buffer.clear();
                bytesRead = this.inputStream.read(this.buffer.array());
            }
            catch (IOException exp) {
                if (this.closeCalled) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)"Got exception because of close() is called in different thread");
                    }
                    return null;
                }
                this.logger.error((Object)("File : {" + this.name + "}"));
                this.logger.error((Object)("FileReader IOException : {" + exp.getMessage() + "}"));
                throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)exp);
            }
            if (bytesRead >= 0) {
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)(bytesRead + " bytes have been read from file " + this.name() + " in the directory " + this.prop.directory));
                }
                this.buffer.limit(bytesRead);
                this.bytesConsumed += bytesRead;
                this.bytesRead += bytesRead;
                this.recoveryCheckpoint.setBytesRead(this.bytesRead);
                this.eofStatus = false;
                return this.buffer;
            }
            this.eofStatus = true;
            if (!this.isEOFReached()) {
                return null;
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        this.closeCalled = true;
        if (this.inputStream != null) {
            this.inputStream.close();
            this.printCloseTrace();
            if (this.deleteAfterUse && !((File)this.currentFile).delete()) {
                this.logger.warn((Object)("Failed to delete {" + ((File)this.currentFile).toString() + "}"));
            }
            this.updateFileMetadataEntry();
        }
        if (this.fb != null) {
            this.fb.stop(this.prop.wildcard);
        }
        this.notifyObservers(Constant.eventType.ON_CLOSE);
    }
    
    @Override
    public long skip(final long bytesToSkip) throws IOException {
        if (this.inputStream != null) {
            return this.inputStream.skip(bytesToSkip);
        }
        return -1L;
    }
    
    protected boolean openFile() throws AdapterException {
        final FileBank.FileDetails fd = this.fb.getNextFile(this.prop.wildcard, (File)this.currentFile, this.bytesConsumed);
        if (fd != null) {
            if (this.inputStream != null) {
                this.printCloseTrace();
                try {
                    this.inputStream.close();
                    if (this.deleteAfterUse) {
                        if (this.logger.isInfoEnabled()) {
                            this.logger.info((Object)("Deleting the file " + ((File)this.currentFile).getName()));
                        }
                        if (!((File)this.currentFile).delete()) {
                            this.logger.warn((Object)("Failed to delete {" + ((File)this.currentFile).toString() + "}"));
                        }
                    }
                }
                catch (IOException e) {
                    throw new AdapterException("Got exception while closing inputstream {" + ((File)this.currentFile).getName() + "}", (Throwable)e);
                }
            }
            final Object prevFile = this.currentFile;
            this.currentFile = fd.file;
            this.currentFileName = fd.file.getName();
            this.logger.debug((Object)("Got file {" + fd.file.getName() + "} position {" + fd.startPosition + "} from Sequencer"));
            if (this.open(this.currentFile)) {
                if (prevFile != null) {
                    this.lastRolledOverFile = " " + ((File)prevFile).getName();
                    this.lastRolledOverTime = " " + new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").format(new Date());
                    if (this.logger.isInfoEnabled()) {
                        final String message = "Processed  {" + ((File)prevFile).getPath() + "} and moved to next file {" + ((File)this.currentFile).getPath() + "}";
                        this.logger.info((Object)message);
                    }
                }
                this.fileChanged = false;
                final long startPosition = fd.startPosition;
                if (startPosition != 0L) {
                    try {
                        this.inputStream.skip(startPosition);
                    }
                    catch (IOException e2) {
                        throw new AdapterException("Got exception while positioning inputstream for {" + startPosition + "}", (Throwable)e2);
                    }
                }
                this.bytesConsumed = startPosition;
                this.recoveryCheckpoint.seekPosition(startPosition);
                this.eventMetadataMap.put("FileOffset", startPosition);
                this.logger.debug((Object)("Setting BytesConsumed as {" + this.bytesConsumed + "}"));
                return true;
            }
            this.logger.debug((Object)("File open failed for {" + ((File)this.currentFile).getName() + "} put back the old file"));
            this.currentFile = prevFile;
        }
        return false;
    }
    
    protected boolean open(final Object file) throws AdapterException {
        if (file == null) {
            return false;
        }
        boolean fileOpen = true;
        final File fileToBeOpened = (File)file;
        this.name(fileToBeOpened.getName());
        try {
            this.inputStream = new FileInputStream(fileToBeOpened);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("File [" + fileToBeOpened.getName() + "] opened from the directory [" + this.prop.directory + "]"));
            }
            this.onOpen(fileToBeOpened.getName());
            this.setChanged();
            this.notifyObservers(Constant.eventType.ON_OPEN);
            this.setFileDetails(fileToBeOpened);
            this.createOrUpdateFileMetadataEntry(fileToBeOpened.getName(), fileToBeOpened.getParent());
            return true;
        }
        catch (FileNotFoundException e) {
            final String errMsg = "File :{" + fileToBeOpened.getName() + "} is not found";
            this.logger.warn((Object)errMsg);
            final AdapterException se = new AdapterException(errMsg, (Throwable)e);
            throw se;
        }
        catch (AdapterException e2) {
            if (e2.getType() != Error.BROKEN_LINK) {
                throw e2;
            }
            this.logger.warn((Object)("Got SourceException {" + e2.getMessage() + "} while opening {" + fileToBeOpened.getName() + "}"));
            fileOpen = false;
            this.inputStream = null;
            return fileOpen;
        }
    }
    
    protected boolean isEOFReached() throws AdapterException {
        try {
            if (this.prevBytesConsumed != this.bytesConsumed) {
                this.prevBytesConsumed = this.bytesConsumed;
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)("Reached EOF of {" + ((File)this.currentFile).getPath() + "}"));
                }
            }
            if (this.fb.isEOFReached(this.prop.wildcard, this.inputStream, this.fileChanged)) {
                this.onClose(this.name());
                this.notifyObservers(Constant.eventType.ON_CLOSE);
                this.updateFileMetadataEntry();
                if (this.openFile()) {
                    this.prevBytesConsumed = 0L;
                    return true;
                }
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
    
    @Override
    public long getEOFPosition() {
        return this.eofPosition;
    }
    
    public long getCreationTime() {
        return this.creationTime;
    }
    
    @Override
    public InputStream getInputStream() throws AdapterException {
        if (this.isFirstTime) {
            this.isFirstTime = false;
            if (this.inputStream != null) {
                return this.inputStream;
            }
        }
        if (this.openFile()) {
            return this.inputStream;
        }
        return null;
    }
    
    private void setFileDetails(final File file) throws AdapterException {
        final java.nio.file.Path filePath = file.toPath();
        try {
            final BasicFileAttributes attributes = Files.readAttributes(filePath, BasicFileAttributes.class, new LinkOption[0]);
            this.eofPosition = 0L;
            if (this.readFromSubDirectories) {
                this.eventMetadataMap.put("FileName", file.getCanonicalPath());
            }
            else {
                this.eventMetadataMap.put("FileName", file.getName());
            }
            if (this.positionByEOF) {
                this.eofPosition = attributes.size();
                this.eventMetadataMap.put("FileOffset", this.eofPosition);
            }
            else {
                this.eventMetadataMap.put("FileOffset", 0);
            }
            this.creationTime = attributes.creationTime().toMillis();
            if (this.readFromSubDirectories) {
                this.recoveryCheckpoint.setSourceName(file.getCanonicalPath());
            }
            else {
                this.recoveryCheckpoint.setSourceName(file.getName());
            }
            this.recoveryCheckpoint.setSourceCreationTime(attributes.creationTime().toMillis());
            if (attributes.fileKey() != null) {
                this.fileKey = attributes.fileKey().toString();
            }
            else {
                this.fileKey = "";
            }
        }
        catch (IOException e) {
            if (e instanceof NoSuchFileException && Files.isSymbolicLink(filePath)) {
                this.logger.warn((Object)("Broken link found " + file.getAbsolutePath()));
                throw new AdapterException("Symbolic link {" + file.getName() + "} is broken", (Throwable)e);
            }
            final String errMsg = "Got IOException while retriving file attribute of {" + file.getName() + "}";
            this.logger.warn((Object)errMsg);
            throw new AdapterException(errMsg, (Throwable)e);
        }
    }
    
    @Override
    public long skipBytes(final long offset) throws AdapterException {
        try {
            if (this.inputStream != null) {
                ((FileInputStream)this.inputStream).getChannel().position(offset);
            }
        }
        catch (IOException e) {
            final String errMsg = "Got IOException while positioning file stream {" + this.name() + "} offset {" + offset + "}";
            this.logger.warn((Object)errMsg);
            throw new AdapterException(errMsg, (Throwable)e);
        }
        return offset;
    }
    
    @Override
    public void position(final CheckpointDetail record, final boolean position) throws AdapterException {
        if (record != null) {
            record.dump();
            this.recovery = true;
            this.recoveryCheckpoint.setRecovery(this.recovery);
            if (record.getRecordEndOffset() != null) {
                this.recoveryCheckpoint.setRecordEndOffset((long)record.getRecordBeginOffset());
            }
            this.fileToBeRecovered = record.getSourceName();
            if (!this.fb.position(this.prop.wildcard, this.fileToBeRecovered) || !this.openFile()) {
                final String errMsg = "Trying to recovery from file {" + this.name() + "} which is not found";
                this.logger.warn((Object)errMsg);
                final AdapterException se = new AdapterException(errMsg);
                throw se;
            }
            if (position) {
                this.validateSource(record);
                this.skipBytes(record.seekPosition());
                this.recoveryCheckpoint.seekPosition((long)record.seekPosition());
            }
        }
        else {
            this.recoveryCheckpoint.setRecovery(this.recovery);
            if (this.positionByEOF) {
                FileBank.FileDetails fd;
                do {
                    fd = this.fb.getNextFile(this.prop.wildcard, null, 0L);
                    if (fd != null) {
                        this.currentFile = fd.file;
                    }
                } while (fd != null);
                if (this.currentFile == null) {
                    this.positionByEOF = false;
                    return;
                }
            }
            try {
                if ((this.positionByEOF && this.currentFile != null && !this.open(this.currentFile)) || (!this.positionByEOF && !this.openFile())) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)"Input directory is empty");
                    }
                    this.startFromBegining = true;
                }
                else {
                    if ((this.positionByEOF || position) && !this.startFromBegining) {
                        if (this.currentFile instanceof File) {
                            this.setFileDetails((File)this.currentFile);
                        }
                        this.skipBytes(this.eofPosition);
                        if (this.currentFile instanceof File) {
                            this.logger.debug((Object)("Positioned {" + ((File)this.currentFile).getName() + "} at {" + this.eofPosition + "}"));
                        }
                        this.bytesConsumed = this.eofPosition;
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
    
    protected void validateSource(final CheckpointDetail record) throws AdapterException {
        if (this.eofPosition < record.seekPosition()) {
            final String errMsg = "File {" + this.name() + "} seems to be truncated. Trying to position at {" + record.seekPosition() + "} but it has got only  {" + this.eofPosition + "} bytes";
            this.logger.warn((Object)errMsg);
            throw new AdapterException(errMsg);
        }
    }
    
    protected void printCloseTrace() {
        if (this.logger.isTraceEnabled()) {
            if (this.readByteAddress != 0L) {
                this.readByteAddress = this.bytesRead - this.previousBytesRead;
                this.previousBytesRead = this.bytesRead;
            }
            else {
                final long bytesRead = this.bytesRead;
                this.previousBytesRead = bytesRead;
                this.readByteAddress = bytesRead;
            }
            this.logger.trace((Object)("File [" + this.name() + "] has reached its EOF position [" + this.readByteAddress + "] and is closed"));
        }
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        return this.eventMetadataMap;
    }
    
    @Override
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        events.add(MonitorEvent.Type.LAST_FILE_READ, this.lastRolledOverFile);
        events.add(MonitorEvent.Type.LAST_FILE_READ_TIME, this.lastRolledOverTime);
        events.add(MonitorEvent.Type.CURRENT_FILENAME, this.currentFileName);
        events.add(MonitorEvent.Type.EOF_STATUS, " " + this.eofStatus);
        events.add(MonitorEvent.Type.LAST_READ_FILE_OFFSET, " " + this.bytesRead);
    }
    
    protected void createOrUpdateFileMetadataEntry(final String filename, final String directoryName) {
        final Set<FileMetadataExtension> list = (Set<FileMetadataExtension>)this.fileMetadataExtensionRepository.queryFileMetadataExtension(HDSecurityManager.TOKEN, new FileMetadataFilter(this.componentName, this.componentUUID, directoryName, this.distributionID));
        if (list != null && list.size() > 0) {
            final Iterator<FileMetadataExtension> iterator = list.iterator();
            boolean isEntryInTheRepo = false;
            while (iterator.hasNext()) {
                final FileMetadataExtension fileMetadataExtension = iterator.next();
                if (fileMetadataExtension.getFileName().equals(filename) && directoryName.equals(fileMetadataExtension.getDirectoryName())) {
                    long wrapNo = fileMetadataExtension.getWrapNumber();
                    this.fileMetadataExtension = fileMetadataExtension;
                    isEntryInTheRepo = true;
                    if (fileMetadataExtension.getStatus().equalsIgnoreCase(FileMetadataExtension.Status.COMPLETED.toString())) {
                        ++wrapNo;
                        fileMetadataExtension.setWrapNumber(wrapNo);
                        fileMetadataExtension.setStatus(FileMetadataExtension.Status.PROCESSING.toString());
                        this.fileMetadataExtensionRepository.putFileMetadataExtension(HDSecurityManager.TOKEN, fileMetadataExtension);
                        break;
                    }
                    continue;
                }
            }
            if (!isEntryInTheRepo) {
                this.createFileMetadataEntry(filename, directoryName);
            }
        }
        else {
            this.createFileMetadataEntry(filename, directoryName);
        }
    }
    
    private void createFileMetadataEntry(final String filename, final String directoryName) {
        if (!this.enableFLM) {
            return;
        }
        (this.fileMetadataExtension = new FileMetadataExtension()).setCreationTimeStamp(System.currentTimeMillis());
        this.fileMetadataExtension.setDirectoryName(directoryName);
        this.fileMetadataExtension.setExternalFileCreationTime(this.creationTime);
        this.fileMetadataExtension.setFileName(filename);
        this.fileMetadataExtension.setParentComponent(this.componentName);
        this.fileMetadataExtension.setParentComponentUUID(this.componentUUID);
        this.fileMetadataExtension.setStatus(FileMetadataExtension.Status.PROCESSING.toString());
        this.fileMetadataExtension.setWrapNumber(1L);
        this.fileMetadataExtension.setDistributionID(this.distributionID);
        this.fileMetadataExtensionRepository.putFileMetadataExtension(HDSecurityManager.TOKEN, this.fileMetadataExtension);
    }
    
    protected void updateFileMetadataEntry() {
        if (!this.enableFLM) {
            return;
        }
        this.fileMetadataExtension.setReasonForRollOver(FileMetadataExtension.ReasonForRollOver.EOF.toString());
        this.fileMetadataExtension.setRollOverTimeStamp(System.currentTimeMillis());
        this.fileMetadataExtension.setStatus(FileMetadataExtension.Status.COMPLETED.toString());
        this.fileMetadataExtensionRepository.putFileMetadataExtension(HDSecurityManager.TOKEN, this.fileMetadataExtension);
    }
    
    @Override
    public void setComponentName(final String componentName) {
        this.componentName = componentName;
    }
    
    @Override
    public void setComponentUUID(final UUID componentUUID) {
        this.componentUUID = componentUUID;
    }
    
    @Override
    public void setDistributionID(final String distributionID) {
        this.distributionID = distributionID;
    }
    
    @Override
    public FileMetadataExtension getFileMetadataExtension() {
        return this.fileMetadataExtension;
    }
}
