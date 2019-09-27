package com.datasphere.source.lib.directory.util;

import java.nio.file.*;
import java.io.*;

public class DirectoryFilter implements DirectoryStream.Filter<Path>
{
    @Override
    public boolean accept(final Path entry) {
        return entry.toFile().isDirectory();
    }
}
