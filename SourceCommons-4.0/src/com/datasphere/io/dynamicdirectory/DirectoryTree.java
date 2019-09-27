package com.datasphere.io.dynamicdirectory;

import org.apache.log4j.*;

import java.nio.file.*;
import java.lang.reflect.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.utils.lookup.*;
import com.datasphere.event.*;
import java.util.*;
import java.io.*;

public class DirectoryTree
{
    protected String separator;
    private String directoryPrefix;
    private String directorySuffix;
    private String currentDirectoryName;
    protected Map<String, Object> directoryCache;
    private boolean directoryNameMalformed;
    private Logger logger;
    private EventLookup eventLookup;
    private String baseDirectoryName;
    
    public DirectoryTree(final Map<String, Object> properties, final String baseDirectoryName) {
        this.directoryNameMalformed = false;
        this.logger = Logger.getLogger((Class)DirectoryTree.class);
        this.eventLookup = null;
        this.baseDirectoryName = "";
        final Property property = new Property(properties);
        this.directoryCache = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        this.directoryPrefix = property.getString("directoryprefix", "");
        this.directorySuffix = property.getString("directorysuffix", "");
        this.separator = FileSystems.getDefault().getSeparator();
        if (baseDirectoryName != null && !baseDirectoryName.isEmpty()) {
            this.baseDirectoryName = baseDirectoryName;
        }
    }
    
    public void intiliazeEventLookup(final String[] tokens, final Field[] fields) throws Exception {
        this.eventLookup = EventLookupFactory.getEventLookup(fields, tokens);
    }
    
    private String formDirectoryString(final Event event) throws IOException {
        try {
            final StringBuilder directoryToBeCreated = new StringBuilder();
            directoryToBeCreated.append(this.directoryPrefix);
            final Iterator<Object> tokenNameIterator = this.eventLookup.get(event).iterator();
            boolean rootDirectorySet = false;
            while (tokenNameIterator.hasNext()) {
                final String childDirectoryName = this.getValidatedDirectoryTokenValue(tokenNameIterator.next());
                if (!rootDirectorySet) {
                    directoryToBeCreated.append(childDirectoryName);
                    rootDirectorySet = true;
                }
                else {
                    directoryToBeCreated.append(this.separator).append(childDirectoryName);
                }
            }
            return directoryToBeCreated.append(this.directorySuffix).toString();
        }
        catch (Exception e) {
            throw new IOException("Unable to form directory string for creating dynamic directory", e);
        }
    }
    
    public boolean createDirectory(final Event event) throws IOException {
        final String directoryToBeCreated = this.formDirectoryString(event);
        if (this.directoryNameMalformed) {
            this.logger.warn((Object)("Malformed directory " + directoryToBeCreated + ". Discarding event " + event.toString()));
            return false;
        }
        this.currentDirectoryName = directoryToBeCreated;
        return this.directoryCache.get(directoryToBeCreated) == null && this.createDirectory(directoryToBeCreated);
    }
    
    protected boolean createDirectory(final String directoryName) throws IOException {
        final File directory = new File(this.baseDirectoryName + this.currentDirectoryName);
        this.directoryCache.put(this.currentDirectoryName, directory);
        if (directory.mkdirs()) {
            return true;
        }
        if (directory.exists()) {
            return true;
        }
        throw new IOException("Failure in creating directory " + directory.getAbsolutePath());
    }
    
    public String getCurrentDirectoryName() {
        return this.currentDirectoryName;
    }
    
    public boolean isDirectoryNameMalformed() {
        if (this.directoryNameMalformed) {
            this.directoryNameMalformed = false;
            return true;
        }
        return false;
    }
    
    private String getValidatedDirectoryTokenValue(final Object directoryTokenValue) {
        String directoryName = null;
        if (directoryTokenValue != null) {
            directoryName = directoryTokenValue.toString().trim();
            if (directoryName.isEmpty()) {
                this.directoryNameMalformed = true;
            }
        }
        else {
            this.directoryNameMalformed = true;
        }
        return directoryName;
    }
}
