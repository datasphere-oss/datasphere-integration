package com.datasphere.preview;

import org.apache.log4j.*;
import java.util.*;
import java.io.*;

public class SampleDataFiles
{
    private static Logger logger;
    List<DataFile> samples;
    File homeFile;
    boolean samplesListCreated;
    
    public synchronized void init(String home) throws IOException {
        if (home.endsWith("Platform")) {
            home = home.substring(0, home.lastIndexOf("/"));
        }
        final String customerFolder = System.getProperty("com.datasphere.config.samplesHome");
        assert customerFolder != null;
        final File homeDir = new File(home);
        home = homeDir.getCanonicalPath();
        home = home.concat(customerFolder);
        this.samples = new ArrayList<DataFile>();
        for (final DataPreviewConstants.DataFileInfo d : DataPreviewConstants.DataFileInfo.values()) {
            final String pathToFile = home.concat(d.getPath());
            final File file = new File(pathToFile);
            if (file.exists()) {
                final DataFile dataFile = new DataFile();
                dataFile.size = file.length();
                dataFile.name = file.getName();
                dataFile.type = d.getType();
                dataFile.fqPath = pathToFile;
                this.samples.add(dataFile);
            }
        }
    }
    
    public void searchForSampleDataFiles(final File parent, final String nameOfDir) throws IOException {
        if (parent.isDirectory()) {
            if (SampleDataFiles.logger.isInfoEnabled()) {
                SampleDataFiles.logger.info((Object)("Searching " + parent.getAbsolutePath() + " for creating pointers to sample data files in Source Preview"));
            }
            if (parent.canRead()) {
                for (final File temp : parent.listFiles()) {
                    if (temp.isDirectory()) {
                        if (temp.getName().equalsIgnoreCase(nameOfDir)) {
                            if (SampleDataFiles.logger.isInfoEnabled()) {
                                SampleDataFiles.logger.info((Object)("App data found at: " + temp.getAbsolutePath()));
                            }
                            for (final File anotherTemp : temp.listFiles()) {
                                final DataFile dataFile = new DataFile();
                                dataFile.name = anotherTemp.getName();
                                dataFile.fqPath = anotherTemp.getCanonicalPath();
                                dataFile.size = anotherTemp.length();
                                this.samples.add(dataFile);
                            }
                        }
                        else {
                            this.searchForSampleDataFiles(temp, nameOfDir);
                        }
                    }
                }
            }
        }
    }
    
    public List<DataFile> getSamples() {
        return this.samples;
    }
    
    static {
        SampleDataFiles.logger = Logger.getLogger((Class)SampleDataFiles.class);
    }
}
