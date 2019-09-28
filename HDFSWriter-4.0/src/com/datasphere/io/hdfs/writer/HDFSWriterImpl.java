package com.datasphere.io.hdfs.writer;

import org.apache.log4j.*;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.source.lib.prop.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import com.datasphere.common.exc.*;
import com.datasphere.io.dynamicdirectory.*;
import com.datasphere.source.lib.intf.*;
import com.datasphere.wizard.*;
import com.datasphere.io.hdfs.commmon.*;
import com.datasphere.io.hdfs.directory.*;
import com.datasphere.proc.*;

import org.apache.hadoop.conf.*;

public class HDFSWriterImpl extends FileWriter_1_0 implements TargetWizard
{
    private FileSystem hadoopFileSystem;
    private Logger logger;
    private FSDataOutputStream fsDataOutputStream;
    private HDFSCommon hdfsCommon;
    private static final String HADOOP_URL = "hadoopurl";
    
    public HDFSWriterImpl() {
        this.logger = Logger.getLogger((Class)HDFSWriterImpl.class);
    }
    
    public void init(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties, final UUID inputStream, final String distributionID) throws Exception {
        this.hdfsCommon = new HDFSCommon(new Property((Map)writerProperties));
        this.hadoopFileSystem = this.hdfsCommon.getHDFSInstance();
        this.createLocalDirectory = false;
        super.init((Map)writerProperties, (Map)formatterProperties, inputStream, distributionID);
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("HDFSWriter is initialized with following properties\nHadoopURL " + this.hdfsCommon.getHadoopUrl()));
        }
    }
    
    protected OutputStream getOutputStream(final String filename) throws IOException {
        final String url = this.hdfsCommon.getHadoopUrl() + filename;
        final Path file = new Path(url);
        if (this.fileMetadataRepositoryEnabled && this.hadoopFileSystem.exists(file)) {
            throw new IOException("File " + file.getName() + " already exists in the current directory " + file.getParent() + ". Please archive the existing files in this directory starting from this sequence");
        }
        if (this.append) {
            if (!this.hdfsCommon.isAppendEnabled()) {
                throw new IOException("HDFSWriter is started in append mode, this mode requires HadoopConfigurationPath property to be set and please ensure \"dfs.support.append\" value isn't set to false.");
            }
            if (this.hadoopFileSystem.exists(file)) {
                this.fsDataOutputStream = this.hadoopFileSystem.append(file);
            }
            else {
                this.fsDataOutputStream = this.hadoopFileSystem.create(file);
            }
        }
        else {
            this.fsDataOutputStream = this.hadoopFileSystem.create(file);
        }
        this.externalFileCreationTime = this.hadoopFileSystem.getFileStatus(file).getModificationTime();
        return this.fsDataOutputStream;
    }
    
    public void close() throws AdapterException {
        try {
            super.close();
            this.closeHadoopFileSystem();
        }
        catch (AdapterException e) {
            this.closeHadoopFileSystem();
            this.logger.error((Object)("Failure in closing HDFSWriter " + e.getMessage()));
        }
    }
    
    protected void sync() throws IOException {
        this.fsDataOutputStream.flush();
        this.fsDataOutputStream.hsync();
    }
    
    private void closeHadoopFileSystem() throws AdapterException {
        try {
            this.hadoopFileSystem.close();
        }
        catch (IOException e) {
            this.logger.error((Object)("Failure in closing HDFS filesystem" + e.getMessage()));
        }
    }
    
    protected DynamicDirectoryOutputStreamGenerator createDynamicDirectoryOutputStreamGenerator() {
        return new HDFSDynamicDirectoryOutputStreamGenerator(this.rollOverProperty, (RollOverObserver)this, this.hadoopFileSystem);
    }
    
    protected String getSeparator() {
        return "/";
    }
    
    public String validateConnection(final Map<String, Object> request) {
        final ValidationResult result = new ValidationResult();
        try {
            WizardUtil.validateRequest(new String[] { "hadoopurl" }, (Map)request);
        }
        catch (IllegalArgumentException e) {
            result.result = false;
            result.message = e.getMessage();
            return result.toJson();
        }
        final Configuration hdfsConfiguration = new Configuration();
        hdfsConfiguration.set("fs.defaultFS", "hadoopurl");
        try {
            FileSystem.get(hdfsConfiguration);
        }
        catch (IOException e2) {
            result.result = false;
            result.message = e2.getMessage();
            return result.toJson();
        }
        result.result = true;
        result.message = "Connection Successful!!!";
        return result.toJson();
    }
    
    public String list(final Map<String, Object> request) {
        final ValidationResult result = new ValidationResult();
        result.result = false;
        result.message = "Operation not supported!!!";
        return result.toJson();
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
