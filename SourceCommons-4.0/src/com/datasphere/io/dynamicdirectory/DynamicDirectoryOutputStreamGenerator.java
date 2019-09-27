package com.datasphere.io.dynamicdirectory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.intf.Formatter;
import com.datasphere.io.file.persistence.util.FileMetadataPersistenceManager;
import com.datasphere.source.lib.intf.RollOverObserver;
import com.datasphere.source.lib.rollingpolicy.outputstream.RollOverOutputStream;
import com.datasphere.source.lib.rollingpolicy.property.RollOverProperty;
import com.datasphere.source.lib.rollingpolicy.util.DynamicRollOverFilenameFormat;
import com.datasphere.source.lib.rollingpolicy.util.RollOverOutputStreamFactory;
import com.datasphere.source.lib.rollingpolicy.util.RolloverFilenameFormat;

public class DynamicDirectoryOutputStreamGenerator
{
    private boolean dynamicDirectoryOutputStreamGenerationEnabled;
    private DirectoryTree directoryTree;
    private String currentWorkingDirectoryName;
    private Map<String, OutputStream> directoryOutputStreamCache;
    private RollOverOutputStream.OutputStreamBuilder outputStreamBuilder;
    private RollOverObserver rollOverObserver;
    private FileMetadataPersistenceManager fileMetadataPersistenceManager;
    private Map<String, RolloverFilenameFormat> directoryFilenameFormatCache;
    private Map<String, Long> fileEventCounterCache;
    private Formatter formatter;
    private String baseDirectoryName;
    
    public DynamicDirectoryOutputStreamGenerator(final RollOverObserver rollOverObserver) {
        this.dynamicDirectoryOutputStreamGenerationEnabled = false;
        this.directoryTree = null;
        this.currentWorkingDirectoryName = null;
        this.directoryOutputStreamCache = null;
        this.outputStreamBuilder = null;
        this.fileMetadataPersistenceManager = null;
        this.directoryFilenameFormatCache = null;
        this.fileEventCounterCache = null;
        this.formatter = null;
        this.baseDirectoryName = "";
        this.rollOverObserver = rollOverObserver;
    }
    
    public void identifyTokens(final Field[] fields, final String directoryOrFolderName) throws AdapterException {
        if (directoryOrFolderName.contains("%")) {
            this.dynamicDirectoryOutputStreamGenerationEnabled = true;
            this.validateTokenFromIdentifier(directoryOrFolderName, fields);
            this.directoryOutputStreamCache = new HashMap<String, OutputStream>();
            this.directoryFilenameFormatCache = new HashMap<String, RolloverFilenameFormat>();
            this.fileEventCounterCache = new HashMap<String, Long>();
        }
    }
    
    private void validateTokenFromIdentifier(final String identifier, final Field[] fields) throws AdapterException {
        final int count = this.countOccurrencesOfToken(identifier);
        if (count % 2 != 0) {
            throw new AdapterException("Failure in initializing writer. Directory name " + identifier + " is invalid. Valid tokens are of the form %token1%/%token2%/...");
        }
        final int[] tokenIndexArray = new int[count];
        int i = 0;
        int j = 0;
        while (i < identifier.length()) {
            if (identifier.charAt(i) == '%') {
                tokenIndexArray[j] = i;
                ++j;
            }
            ++i;
        }
        final int startIndexOfToken = identifier.indexOf(37);
        final int endIndexOfToken = identifier.lastIndexOf(37);
        String directoryPrefix = "";
        String directorySuffix = "";
        if (startIndexOfToken == 0) {
            if (endIndexOfToken + 1 == identifier.length() - 1) {
                directorySuffix = identifier.substring(endIndexOfToken + 1, identifier.length());
            }
        }
        else if (endIndexOfToken + 1 == identifier.length() - 1) {
            directoryPrefix = identifier.substring(0, startIndexOfToken);
            directorySuffix = identifier.substring(endIndexOfToken + 1, identifier.length());
        }
        else {
            directoryPrefix = identifier.substring(0, startIndexOfToken);
            directorySuffix = identifier.substring(endIndexOfToken + 1, identifier.length());
        }
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("directoryprefix", directoryPrefix);
        properties.put("directorysuffix", directorySuffix);
        final String[] tokens = new String[count / 2];
        for (int k = 0, l = 0; k < count; k += 2, ++l) {
            final String tokenName = identifier.substring(tokenIndexArray[k] + 1, tokenIndexArray[k + 1]);
            tokens[l] = tokenName;
        }
        this.directoryTree = this.createDirectoryTree(properties);
        try {
            this.directoryTree.intiliazeEventLookup(tokens, fields);
        }
        catch (Exception e) {
            throw new AdapterException("Failure in initializing writer while enabling dynamic directory using token(s) specified", (Throwable)e);
        }
    }
    
    protected DirectoryTree createDirectoryTree(final Map<String, Object> properties) {
        return new DirectoryTree(properties, this.baseDirectoryName);
    }
    
    public OutputStream getDynamicOutputStream(final Event event) throws IOException {
        OutputStream dynamicOutputStream;
        if (this.directoryTree.createDirectory(event)) {
            this.currentWorkingDirectoryName = this.directoryTree.getCurrentDirectoryName();
            final RollOverProperty rollOverProperty = this.fileMetadataPersistenceManager.getRolloverProperty(this.currentWorkingDirectoryName);
            final DynamicRollOverFilenameFormat filenameFormat = new DynamicRollOverFilenameFormat(rollOverProperty.getFilename(), rollOverProperty.getFileLimit(), rollOverProperty.getSequenceStart(), rollOverProperty.getIncrementSequenceBy(), rollOverProperty.shouldAddDefaultSequence(), this.currentWorkingDirectoryName);
            this.directoryFilenameFormatCache.put(this.currentWorkingDirectoryName, filenameFormat);
            dynamicOutputStream = RollOverOutputStreamFactory.getRollOverOutputStream(rollOverProperty, this.outputStreamBuilder, filenameFormat);
            ((RollOverOutputStream)dynamicOutputStream).registerObserver(this.rollOverObserver);
            try {
                this.rollOverObserver.notifyOnInitialOpen();
                if (this.formatter != null) {
                    final byte[] bytesToBeWrittenOnOpen = this.formatter.addHeader();
                    if (bytesToBeWrittenOnOpen != null) {
                        dynamicOutputStream.write(bytesToBeWrittenOnOpen);
                    }
                }
            }
            catch (Exception e1) {
                throw new IOException(e1);
            }
            this.directoryOutputStreamCache.put(this.currentWorkingDirectoryName, dynamicOutputStream);
            Long eventCounter = this.getEventCounterForThisFile(this.currentWorkingDirectoryName + filenameFormat.getCurrentFileName());
            ++eventCounter;
            this.fileEventCounterCache.put(this.currentWorkingDirectoryName + filenameFormat.getCurrentFileName(), eventCounter);
        }
        else if (!this.directoryTree.isDirectoryNameMalformed()) {
            final String currentDirectoryName = this.directoryTree.getCurrentDirectoryName();
            final RolloverFilenameFormat filenameFormat2 = this.directoryFilenameFormatCache.get(currentDirectoryName);
            final String currentFilename = filenameFormat2.getCurrentFileName();
            Long eventCounter = this.getEventCounterForThisFile(currentDirectoryName + currentFilename);
            ++eventCounter;
            this.fileEventCounterCache.put(currentDirectoryName + filenameFormat2.getCurrentFileName(), eventCounter);
            if (this.currentWorkingDirectoryName.equalsIgnoreCase(currentDirectoryName)) {
                return this.directoryOutputStreamCache.get(currentDirectoryName);
            }
            this.currentWorkingDirectoryName = currentDirectoryName;
            dynamicOutputStream = this.directoryOutputStreamCache.get(currentDirectoryName);
        }
        else {
            dynamicOutputStream = null;
        }
        return dynamicOutputStream;
    }
    
    private int countOccurrencesOfToken(final String identifier) {
        int count = 0;
        for (int i = 0; i < identifier.length(); ++i) {
            if (identifier.charAt(i) == '%') {
                ++count;
            }
        }
        return count;
    }
    
    public boolean enabled() {
        return this.dynamicDirectoryOutputStreamGenerationEnabled;
    }
    
    public void setOutputStreamBuilder(final RollOverOutputStream.OutputStreamBuilder outputStreamBuilder) {
        this.outputStreamBuilder = outputStreamBuilder;
    }
    
    public String getCurrentWorkingDirectoryName() {
        return this.currentWorkingDirectoryName;
    }
    
    public Map<String, OutputStream> getOutputStreamCache() {
        return this.directoryOutputStreamCache;
    }
    
    public void clear() {
        if (this.dynamicDirectoryOutputStreamGenerationEnabled) {
            this.directoryOutputStreamCache.clear();
            this.directoryOutputStreamCache = null;
            this.directoryFilenameFormatCache.clear();
            this.directoryFilenameFormatCache = null;
            this.fileEventCounterCache.clear();
            this.fileEventCounterCache = null;
        }
    }
    
    public void setFileMetadataPersistenceManager(final FileMetadataPersistenceManager fileMetadataPersistenceManager) {
        this.fileMetadataPersistenceManager = fileMetadataPersistenceManager;
    }
    
    public RolloverFilenameFormat getCachedFilenameFormat(final String key) {
        return this.directoryFilenameFormatCache.get(key);
    }
    
    private Long getEventCounterForThisFile(final String filenameDirectoryKey) {
        Long eventCounter = this.fileEventCounterCache.get(filenameDirectoryKey);
        if (eventCounter != null) {
            return eventCounter;
        }
        eventCounter = 0L;
        this.fileEventCounterCache.put(filenameDirectoryKey, eventCounter);
        return eventCounter;
    }
    
    public Long getEvenCounter(final String filenameDirectoryKey) {
        return this.fileEventCounterCache.get(filenameDirectoryKey);
    }
    
    public void setFormatter(final Formatter formatter) {
        this.formatter = formatter;
    }
    
    public void setBaseDirectoryName(final String baseDirectoryName) {
        this.baseDirectoryName = baseDirectoryName;
    }
}
