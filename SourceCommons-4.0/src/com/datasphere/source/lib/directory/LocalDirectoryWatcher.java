package com.datasphere.source.lib.directory;

import org.apache.log4j.*;

import java.io.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import java.nio.file.attribute.*;
import java.util.concurrent.*;
import java.nio.file.*;
import java.util.*;

public class LocalDirectoryWatcher extends Watcher
{
    Logger logger;
    WatchService watcher;
    WatchKey key;
    
     <T> WatchEvent<T> cast(final WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }
    
    public LocalDirectoryWatcher(final Property prop, final WatcherCallback callback) throws IOException {
        super(prop, callback);
        this.logger = Logger.getLogger((Class)LocalDirectoryWatcher.class);
    }
    
    @Override
    protected void init() throws AdapterException {
        try {
            this.watcher = FileSystems.getDefault().newWatchService();
            this.register(this.dir);
        }
        catch (IOException e) {
            throw new AdapterException("Got exception while initializing WatcherService", (Throwable)e);
        }
    }
    
    private void register(final Path dir) throws IOException {
        this.key = dir.register(this.watcher, (WatchEvent.Kind<?>[])new WatchEvent.Kind[] { StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE });
    }
    
    private void registerRecursively(final Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                LocalDirectoryWatcher.this.register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
    
    @Override
    public void stop() {
        super.stop();
        try {
            this.stopCalled = true;
            this.watcher.close();
            this.logger.debug((Object)"Stopped LocalDirectoryWatcher....");
        }
        catch (IOException e) {
            this.logger.warn((Object)"Got exception while closing watcher service");
        }
    }
    
    @Override
    public void run() {
        while (!this.stopCalled) {
            try {
                this.key = this.watcher.poll(1000L, TimeUnit.MILLISECONDS);
                if (this.key == null || this.stopCalled) {
                    continue;
                }
                for (final WatchEvent<?> event : this.key.pollEvents()) {
                    final WatchEvent.Kind<?> kind = event.kind();
                    final WatchEvent<Path> ev = this.cast(event);
                    final Path name = ev.context();
                    final Path resolvedPath = this.dir.resolve(name);
                    try {
                        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                            if (this.readSubDirectories && Files.isDirectory(resolvedPath, new LinkOption[0])) {
                                this.registerRecursively(resolvedPath);
                            }
                            else {
                                if (!this.matcher.matches(resolvedPath.getFileName())) {
                                    continue;
                                }
                                this.onFileCreate(resolvedPath.toFile());
                            }
                        }
                        else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                            this.onFileModify(resolvedPath.toFile());
                        }
                        else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                            this.onFileDelete(resolvedPath.toFile());
                        }
                        else {
                            if (kind == StandardWatchEventKinds.OVERFLOW || !this.logger.isDebugEnabled()) {
                                continue;
                            }
                            this.logger.debug((Object)"Unhandled watcher event received");
                        }
                    }
                    catch (Exception e) {
                        this.logger.error((Object)("Unexpected error during file watcher operation " + kind.name() + " for the file " + name.getFileName() + ". FileWatcher thread would continue processing/watching other files skipping this file."), (Throwable)e);
                    }
                }
                this.key.reset();
            }
            catch (Exception e2) {
                if (this.stopCalled) {
                    break;
                }
                this.logger.error((Object)"Got exception while polling DirectoryWatcher service", (Throwable)e2);
            }
        }
        this.logger.debug((Object)"Exiting LocalDiretoryWatcher service");
    }
    
    private void foreach(final WatchEvent<?> watchEvent) {
    }
}
