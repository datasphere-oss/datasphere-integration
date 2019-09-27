package com.datasphere.source.lib.directory;

import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import java.io.*;
import java.nio.file.*;

public class MultiFileBank extends FileBank
{
    public MultiFileBank(final Property prop) throws AdapterException {
        super(prop);
    }
    
    @Override
    public void updateInstance(final Property prop) throws AdapterException {
        if (this.getSequencer(prop.wildcard) == null) {
            final FileSequencer sequencer = this.createSequencer(prop);
            this.sequencerMap.put(prop.wildcard, sequencer);
            MultiFileBank.instanceCnt.incrementAndGet();
        }
    }
    
    private void updateObserver(final Subject subject) {
        this.setChanged();
        this.notifyObservers(subject);
    }
    
    protected FileSequencer getSequencer(final File file) {
        final String pGroup = FileBank.extractProcessGroup(this.prop.groupPattern, file);
        final FileSequencer sequencer = this.getSequencer(pGroup);
        if (sequencer == null && MultiFileBank.logger.isDebugEnabled()) {
            MultiFileBank.logger.debug((Object)("Wildcard {" + pGroup + "} has no assiciated sequencer"));
        }
        return sequencer;
    }
    
    protected FileSequencer getSequencer(final String wildcard) {
        final FileSequencer sequencer = this.sequencerMap.get(wildcard);
        if (sequencer == null) {
            MultiFileBank.logger.warn((Object)("No sequencer found for {" + wildcard + "}"));
        }
        return sequencer;
    }
    
    @Override
    public boolean isEOFReached(final String wildcard, final InputStream in, final boolean fileChanged) throws IOException {
        final FileSequencer sequencer = this.getSequencer(wildcard);
        return sequencer == null || sequencer.isEOFReached(in, fileChanged);
    }
    
    @Override
    public boolean isEmpty(final String wildcard) {
        final FileSequencer sequencer = this.getSequencer(wildcard);
        return sequencer == null || sequencer.isEmpty();
    }
    
    @Override
    public FileDetails getNextFile(final String wildcard, final File file, final long position) {
        final FileSequencer sequencer = this.getSequencer(wildcard);
        if (sequencer != null) {
            final File fileObj = sequencer.getNextFile(file, position);
            if (fileObj != null) {
                final FileDetails fd = new FileDetails();
                fd.file = fileObj;
                fd.startPosition = sequencer.getPosition();
                if (MultiFileBank.logger.isDebugEnabled()) {
                    MultiFileBank.logger.debug((Object)("File process {" + fd.file.getName() + "} start position {" + fd.startPosition + "}"));
                }
                return fd;
            }
        }
        return null;
    }
    
    @Override
    public long getPosition(final String wildcard) {
        final FileSequencer sequencer = this.getSequencer(wildcard);
        if (sequencer != null) {
            return sequencer.getPosition();
        }
        return 0L;
    }
    
    @Override
    public boolean position(final String wildcard, final String fileToRecover) {
        final FileSequencer sequencer = this.getSequencer(wildcard);
        return sequencer != null && sequencer.position(fileToRecover);
    }
    
    @Override
    public void onFileCreate(final File file) {
        if (MultiFileBank.logger.isDebugEnabled()) {
            MultiFileBank.logger.debug((Object)("New file created {" + file.getName() + "}"));
        }
        this.subject.file = file;
        this.subject.event = StandardWatchEventKinds.ENTRY_CREATE;
        this.updateObserver(this.subject);
        final FileSequencer sequencer = this.getSequencer(file);
        if (sequencer != null) {
            sequencer.onFileCreate(file);
        }
    }
    
    @Override
    public void onFileDelete(final File file) {
        if (MultiFileBank.logger.isDebugEnabled()) {
            MultiFileBank.logger.debug((Object)("File {" + file.getName() + "} is deleted"));
        }
        this.subject.file = file;
        this.subject.event = StandardWatchEventKinds.ENTRY_DELETE;
        this.updateObserver(this.subject);
        final FileSequencer sequencer = this.getSequencer(file);
        if (sequencer != null) {
            sequencer.onFileDelete(file);
        }
    }
    
    @Override
    public void onFileDelete(final String fileKey, final String fileName) {
        if (MultiFileBank.logger.isDebugEnabled()) {
            MultiFileBank.logger.debug((Object)("File {" + fileKey + "} {" + fileName + "} is deleted"));
        }
        this.subject.file = new File(fileName);
        this.subject.event = StandardWatchEventKinds.ENTRY_DELETE;
        this.updateObserver(this.subject);
        final FileSequencer sequencer = this.getSequencer(fileName);
        if (sequencer != null) {
            sequencer.onFileDelete(fileKey, fileName);
        }
    }
    
    @Override
    public void onFileModify(final File file) {
        if (MultiFileBank.logger.isDebugEnabled()) {
            MultiFileBank.logger.debug((Object)("File {" + file.getName() + "} is modified"));
        }
        this.subject.file = file;
        this.subject.event = StandardWatchEventKinds.ENTRY_MODIFY;
        this.setChanged();
        this.updateObserver(this.subject);
        final FileSequencer sequencer = this.getSequencer(file);
        if (sequencer != null) {
            sequencer.onFileModify(file);
        }
    }
}
