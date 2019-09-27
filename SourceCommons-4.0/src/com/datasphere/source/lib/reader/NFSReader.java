package com.datasphere.source.lib.reader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;

public class NFSReader extends FileReader
{
    Logger logger;
    protected long fileSize;
    protected int bytesToRead;
    protected long lastFileSize;
    boolean checkForNullByte;
    int nfsReadDelay;
    boolean flag;
    long prevBytesConsumed;
    int lastBytesRead;
    String curFile;
    String tmpFile;
    FileOutputStream tmpStream;
    
    public NFSReader(final Property prop) throws AdapterException {
        super(prop);
        this.logger = Logger.getLogger((Class)NFSReader.class);
        this.checkForNullByte = false;
        this.nfsReadDelay = 50;
        this.flag = true;
        this.checkForNullByte = prop.getBoolean("checkfornullbyte", false);
        this.nfsReadDelay = prop.getInt("nfsreaddelay", 50);
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        if (this.inputStream == null) {
            this.position(null, false);
            return null;
        }
        while (true) {
            this.fileChanged = false;
            Label_0376: {
                if (this.bytesConsumed < this.fileSize) {
                    if (this.lastBytesRead != -1) {
                        break Label_0376;
                    }
                }
                try {
                    final BasicFileAttributes attributes = Files.readAttributes(((File)this.currentFile).toPath(), BasicFileAttributes.class, new LinkOption[0]);
                    if (!this.fileKey.equals(attributes.fileKey().toString())) {
                        this.fileChanged = true;
                    }
                    else {
                        this.fileSize = attributes.size();
                        if (this.lastFileSize != this.fileSize) {
                            this.flag = true;
                            this.lastFileSize = this.fileSize;
                            try {
                                Thread.sleep(this.nfsReadDelay);
                            }
                            catch (InterruptedException ex) {}
                            if (this.logger.isDebugEnabled()) {
                                this.logger.debug((Object)("FileSize {" + this.fileSize + "} BytesConsumed {" + this.bytesConsumed + "} {" + this.name() + "}"));
                            }
                        }
                        else {
                            if (this.flag) {
                                if (this.logger.isInfoEnabled()) {
                                    this.logger.info((Object)("Waiting for more data {" + this.name() + "}"));
                                }
                                this.logger.debug((Object)("Waiting for more data {" + this.name() + "}"));
                                this.flag = false;
                            }
                            try {
                                Thread.sleep(this.nfsReadDelay);
                            }
                            catch (InterruptedException ex2) {}
                        }
                    }
                }
                catch (NoSuchFileException noSuchFileExp) {
                    this.logger.warn((Object)("File {" + ((File)this.currentFile).toPath() + "} seems to be deleted, proceeding with next file"));
                    this.fileChanged = true;
                }
                catch (IOException e) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)"Error while getting file attribute, will move proceed with the next file");
                    }
                    this.fileChanged = true;
                }
            }
            this.bytesToRead = (int)(this.fileSize - this.bytesConsumed);
            this.bytesToRead = ((this.bytesToRead > this.blockSize) ? this.blockSize : this.bytesToRead);
            int bytes = 0;
            if (this.bytesToRead < 0) {
                this.logger.debug((Object)"Negative BytesToRead");
            }
            if (!this.fileChanged && this.bytesToRead > 0) {
                try {
                    this.buffer.clear();
                    if (this.prevBytesConsumed != this.bytesConsumed) {
                        this.logger.debug((Object)("Trying to read {" + this.bytesToRead + "} from {" + this.name() + "} Key {" + this.fileKey + "} bytesConsumed {" + this.bytesConsumed + "}"));
                        this.prevBytesConsumed = this.bytesConsumed;
                    }
                    bytes = this.inputStream.read(this.buffer.array(), 0, this.bytesToRead);
                    if ((this.lastBytesRead = bytes) > 0) {
                        this.logger.debug((Object)("Read {" + bytes + "} from {" + this.name() + "} Key {" + this.fileKey + "} bytesConsumed {" + this.bytesConsumed + "}"));
                    }
                }
                catch (IOException exp) {
                    if (this.closeCalled) {
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)"Got exception because of close() is called in different thread");
                        }
                        return null;
                    }
                    this.fileChanged = true;
                    this.logger.error((Object)("File : {" + this.name + "} FileReader IOException : {" + exp.getMessage() + "}"));
                    throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)exp);
                }
            }
            if (bytes > 0) {
                this.logger.debug((Object)("File {" + this.name() + "} Key {" + this.fileKey + "} Bytes read {" + bytes + "} bytesConsumed so far {" + this.bytesConsumed + "} fileSize {" + this.fileSize + "}"));
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)(this.bytesRead + " bytes have been read from file " + this.name() + " in the directory " + this.prop.directory));
                }
                if (this.checkForNullByte) {
                    final byte[] buf = this.buffer.array();
                    for (int itr = 0; itr < bytes; ++itr) {
                        if (buf[itr] == 0) {
                            try {
                                this.logger.warn((Object)("Seen NULL byte while reading {" + this.name() + "} File offset {" + (this.bytesRead + itr) + "} block {" + bytes + "}"));
                                if (this.curFile == null || !this.curFile.equals(((File)this.currentFile).getName())) {
                                    this.curFile = ((File)this.currentFile).getName();
                                    this.tmpFile = "/tmp/" + ((File)this.currentFile).getName();
                                    this.logger.debug((Object)("Creating tmp file {" + this.tmpFile + "}"));
                                    if (this.tmpStream != null) {
                                        this.tmpStream.close();
                                    }
                                    this.tmpStream = new FileOutputStream(this.tmpFile);
                                }
                                this.tmpStream.write(this.buffer.array(), 0, bytes);
                                this.tmpStream.flush();
                                break;
                            }
                            catch (IOException exp2) {
                                this.logger.debug((Object)"Got exception while writing NULL block in to /tmp directory");
                            }
                        }
                    }
                }
                this.buffer.limit(bytes);
                this.bytesConsumed += bytes;
                this.bytesRead += bytes;
                this.recoveryCheckpoint.setBytesRead(this.bytesRead);
                return this.buffer;
            }
            if (!this.isEOFReached()) {
                return null;
            }
            this.logger.debug((Object)("Rolled over to next file {" + this.name() + "} key {" + this.fileKey + "}"));
        }
    }
}
