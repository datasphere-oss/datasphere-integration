package com.datasphere.intf;

import com.datasphere.fileSystem.*;
import com.datasphere.preview.*;
/*
 * HDFS文件信息
 */
public interface HDFSFileInfo extends FileSystemBrowser
{
    DataFile getFileData(final String p0, final String p1) throws Exception;
}
