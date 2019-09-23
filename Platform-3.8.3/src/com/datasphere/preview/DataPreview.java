package com.datasphere.preview;

import java.util.List;
import java.util.Map;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.proc.records.Record;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;

public interface DataPreview
{
    List<Record> getRawData(final String p0, final ReaderType p1) throws Exception;
    
    String getFileType(final String p0);
    
    DataFile getFileInfo(final String p0, final ReaderType p1) throws Exception;
    
    List<Map<String, Object>> analyze(final ReaderType p0, final String p1, final ParserType p2) throws Exception;
    
    List<Record> doPreview(final ReaderType p0, final String p1, final Map<String, Object> p2) throws Exception;
    
    MetaInfo.PropertyTemplateInfo getPropertyTemplate(final String p0) throws MetaDataRepositoryException;
}
