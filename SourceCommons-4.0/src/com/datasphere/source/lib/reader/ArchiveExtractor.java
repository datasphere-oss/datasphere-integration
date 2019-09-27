package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;

public class ArchiveExtractor extends StreamReader
{
    private ArchiveInputStream archiveStream;
    
    protected ArchiveExtractor(final ReaderBase link) throws AdapterException {
        super(link);
    }
    
    public void init() throws AdapterException {
        super.init();
        try {
            final String type = this.linkedStrategy.property().getString(Property.ARCHIVE_TYPE, "");
            this.archiveStream = new ArchiveStreamFactory().createArchiveInputStream(type, (InputStream)new Reader(this.linkedStrategy));
            this.dataSource = this.getStream(this.archiveStream);
        }
        catch (ArchiveException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        try {
            if (this.dataSource.available() == 0) {
                this.dataSource = this.getStream(this.archiveStream);
            }
            return super.readBlock();
        }
        catch (IOException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
    
    private InputStream getStream(final ArchiveInputStream stream) throws AdapterException {
        try {
            if (this.archiveStream != null) {
                if (this.name() != null) {
                    this.onClose(this.name());
                }
                final ArchiveEntry entry = stream.getNextEntry();
                if (entry != null) {
                    if (entry.isDirectory()) {
                        return this.getStream(stream);
                    }
                    this.onOpen(entry.getName());
                    this.name(entry.getName());
                    return (InputStream)stream;
                }
            }
            return null;
        }
        catch (IOException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
}
