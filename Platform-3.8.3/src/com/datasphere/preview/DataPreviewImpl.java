package com.datasphere.preview;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.event.Event;
import com.datasphere.intf.Analyzer;
import com.datasphere.intf.HDFSFileInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.datasphere.common.exc.RecordException;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.AbstractEventSink;
import com.datasphere.proc.BaseProcess;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.records.Record;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class DataPreviewImpl implements DataPreview
{
    private static Logger logger;
    public static String MODULE_NAME;
    private final int numberOfEventsForRawData = 20;
    private final int numberOfEventsForPreview = 100;
    private MDRepository mdRepo;
    public static final String ENABLE_FLM = "enableFLM";
    
    public DataPreviewImpl() {
        this.mdRepo = MetadataRepository.getINSTANCE();
    }
    
    @Override
    public List<Record> getRawData(final String fqPathToFile, final ReaderType fType) throws Exception {
        Class<?> clazz = null;
        final Map<String, Object> resultMap = new HashMap<String, Object>();
        switch (fType) {
            case HDFSReader: {
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.HDFSReader_1_0");
                break;
            }
            case FileReader: {
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.FileReader_1_0");
                break;
            }
        }
        final String[] dirAndFileName = this.getDirectoryAndFileName(fqPathToFile);
        final BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
        final Map<String, Object> readerProperties = fType.getDefaultReaderProperties();
        readerProperties.put("enableFLM", false);
        if (fType.equals(ReaderType.HDFSReader)) {
            readerProperties.put("hadoopurl", dirAndFileName[0] = dirAndFileName[0]);
            readerProperties.put("wildcard", dirAndFileName[1]);
            readerProperties.put("adapterName", "HDFSReader");
        }
        else if (fType.equals(ReaderType.FileReader)) {
            readerProperties.put("directory", dirAndFileName[0]);
            readerProperties.put("wildcard", dirAndFileName[1]);
        }
        final Map<String, Object> parserProperties = ParserType.DSV.getDefaultParserProperties();
        final BaseProcess baseProcess2 = baseProcess;
        final Map<String, Object> readerProperties2 = readerProperties;
        final Map<String, Object> parserProperties2 = parserProperties;
        this.getClass();
        final List<Record> allEvents = this.runBaseProcess(baseProcess2, readerProperties2, parserProperties2, 20);
        resultMap.put("Events", allEvents);
        return allEvents;
    }
    
    @Override
    public DataFile getFileInfo(final String fqPathToFile, final ReaderType fType) throws Exception {
        DataFile dataFile = null;
        switch (fType) {
            case FileReader: {
                final File file = new File(fqPathToFile);
                if (file.isFile()) {
                    dataFile = new DataFile();
                    dataFile.name = file.getName();
                    dataFile.fqPath = file.getCanonicalPath();
                    dataFile.size = file.length();
                    break;
                }
                throw new FileNotFoundException("The URI: " + fqPathToFile + " is not a valid path to a file");
            }
            case HDFSReader: {
                final String[] dirAndFileName = this.getDirectoryAndFileName(fqPathToFile);
                final HDFSFileInfo fileInfo = (HDFSFileInfo)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.fileSystem.HDFSBrowserImpl").newInstance();
                final ClassLoader originalClazz = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(fileInfo.getClass().getClassLoader());
                dataFile = fileInfo.getFileData(dirAndFileName[0], fqPathToFile);
                Thread.currentThread().setContextClassLoader(originalClazz);
                break;
            }
        }
        return dataFile;
    }
    
    @Override
    public String getFileType(final String fqPathToFile) {
        return ParserType.DSV.name();
    }
    
    private String[] getDirectoryAndFileName(final String fqPathToFile) {
        final int lastIndex = fqPathToFile.lastIndexOf("/");
        final String fileName = fqPathToFile.substring(lastIndex + 1);
        final String directory = fqPathToFile.substring(0, lastIndex + 1);
        return new String[] { directory, fileName };
    }
    
    @Override
    public List<Map<String, Object>> analyze(final ReaderType rType, final String fqPath, final ParserType pType) throws Exception {
        final Map<String, Object> parserProperties = new HashMap<String, Object>();
        Class<?> clazz = null;
        final String[] dirAndFileName = this.getDirectoryAndFileName(fqPath);
        final Map<String, Object> readerProperties = rType.getDefaultReaderProperties();
        readerProperties.put("enableFLM", false);
        switch (rType) {
            case HDFSReader: {
                readerProperties.put("hadoopurl", dirAndFileName[0]);
                readerProperties.put("wildcard", dirAndFileName[1]);
                readerProperties.put("adapterName", "HDFSReader");
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.HDFSReader_1_0");
                break;
            }
            case FileReader: {
                readerProperties.put("directory", dirAndFileName[0]);
                readerProperties.put("wildcard", dirAndFileName[1]);
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.FileReader_1_0");
                break;
            }
        }
        parserProperties.put(DataPreviewImpl.MODULE_NAME, "Analyzer");
        if (pType != null) {
            parserProperties.put("docType", pType.toString());
        }
        final BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
        final ClassLoader originalClazz = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(baseProcess.getClass().getClassLoader());
        baseProcess.init(readerProperties, parserProperties, null, BaseServer.getServerName(), null, false, null);
        baseProcess.receiveImpl(0, null);
        List<Map<String, Object>> possibleProperties;
        if (pType != null) {
            possibleProperties = ((Analyzer)baseProcess).getProbableProperties();
        }
        else {
            possibleProperties = new ArrayList<Map<String, Object>>();
            final Map<String, Object> fileDetails = ((Analyzer)baseProcess).getFileDetails();
            possibleProperties.add(fileDetails);
        }
        if (DataPreviewImpl.logger.isDebugEnabled()) {
            DataPreviewImpl.logger.debug((Object)("Delimiter recommendations: " + possibleProperties));
        }
        baseProcess.close();
        Thread.currentThread().setContextClassLoader(originalClazz);
        return possibleProperties;
    }
    
    @Override
    public List<Record> doPreview(final ReaderType rType, final String fqPath, final Map<String, Object> parserProperties) throws Exception {
        Class<?> clazz = null;
        List<Record> allEvents = null;
        final String[] dirAndFileName = this.getDirectoryAndFileName(fqPath);
        final Map<String, Object> readerProperties = rType.getDefaultReaderProperties();
        readerProperties.put("enableFLM", false);
        switch (rType) {
            case FileReader: {
                readerProperties.put("directory", dirAndFileName[0]);
                readerProperties.put("wildcard", dirAndFileName[1]);
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.FileReader_1_0");
                break;
            }
            case HDFSReader: {
                readerProperties.put("hadoopurl", dirAndFileName[0]);
                readerProperties.put("wildcard", dirAndFileName[1]);
                readerProperties.put("adapterName", "HDFSReader");
                clazz = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.HDFSReader_1_0");
                break;
            }
        }
        if (clazz != null) {
            final BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
            final String parserHandler = (String)parserProperties.get("handler");
            if (parserHandler.equals("XMLParser") || parserHandler.equals("JSONParser")) {
                readerProperties.remove("charset");
            }
            if (DataPreviewImpl.logger.isDebugEnabled()) {
                DataPreviewImpl.logger.debug((Object)("Parser Properties : " + parserProperties.entrySet()));
            }
            final BaseProcess baseProcess2 = baseProcess;
            final Map<String, Object> readerProperties2 = readerProperties;
            this.getClass();
            allEvents = this.runBaseProcess(baseProcess2, readerProperties2, parserProperties, 100);
        }
        return allEvents;
    }
    
    public List<Record> runBaseProcess(final BaseProcess baseProcess, final Map<String, Object> readerProperties, final Map<String, Object> parserProperties, final int numberOfEvents) throws Exception {
        parserProperties.put("breakonnorecord", true);
        final ClassLoader originalClazz = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(baseProcess.getClass().getClassLoader());
        baseProcess.init(readerProperties, parserProperties, null, BaseServer.getServerName(), null, false, null);
        final List<Record> allEvents = new ArrayList<Record>(numberOfEvents);
        final AtomicBoolean done = new AtomicBoolean(false);
        int cnt = 0;
        baseProcess.addEventSink(new AbstractEventSink() {
            int count = 0;
            
            @Override
            public void receive(final int channel, final Event event) throws Exception {
                Record Record;
                if (event instanceof JsonNodeEvent) {
                    Record = new Record(1, (UUID)null);
                    Record.data = new Object[1];
                    final JsonNode jNode = ((JsonNodeEvent)event).data;
                    Record.data[0] = jNode.toString();
                }
                else {
                    Record = (Record)event;
                }
                ++this.count;
                if (this.count <= numberOfEvents) {
                    if (Record != null) {
                        allEvents.add(Record);
                    }
                }
                else {
                    done.set(true);
                }
            }
        });
        while (!done.get()) {
            try {
                baseProcess.receive(0, null);
            }
            catch (Exception exp) {
                if (exp instanceof RecordException) {
                    final RecordException rExp = (RecordException)exp;
                    if (rExp.type() != RecordException.Type.NO_RECORD) {
                        DataPreviewImpl.logger.warn((Object)"Got exception while loading file for preview", (Throwable)rExp);
                        throw new RuntimeException((Throwable)rExp);
                    }
                    if (cnt >= 3) {
                        break;
                    }
                    Thread.sleep(50L);
                    ++cnt;
                }
                else {
                    DataPreviewImpl.logger.warn((Object)("Got exception: " + exp.getMessage()));
                }
            }
        }
        baseProcess.close();
        Thread.currentThread().setContextClassLoader(originalClazz);
        return allEvents;
    }
    
    @Override
    public MetaInfo.PropertyTemplateInfo getPropertyTemplate(final String name) throws MetaDataRepositoryException {
        return (MetaInfo.PropertyTemplateInfo)this.mdRepo.getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", name, null, HSecurityManager.TOKEN);
    }
    
    public static void main(final String[] args) {
        final DataPreview dataPreview = new DataPreviewImpl();
        final String fqPath = "../Samples/AppData/DataCenterData.csv";
        try {
            final List<Record> allEvents = dataPreview.getRawData(fqPath, ReaderType.FileReader);
            final JSONArray jArray = new JSONArray();
            for (final Record Record : allEvents) {
                jArray.put((Object)Record.toJSON());
            }
            System.out.println("Raw Data: " + allEvents.toString());
            final DataFile dataFile = dataPreview.getFileInfo(fqPath, ReaderType.FileReader);
            System.out.println("File Info: " + dataFile.toJson().toString());
            final List<Map<String, Object>> possibleDocType = dataPreview.analyze(ReaderType.FileReader, fqPath, null);
            final JSONObject jsonObject = new JSONObject();
            for (int itr = 0; itr < possibleDocType.size(); ++itr) {
                final JSONObject docJSon = new JSONObject();
                for (final Map.Entry<String, Object> entrySet : possibleDocType.get(itr).entrySet()) {
                    docJSon.put((String)entrySet.getKey(), entrySet.getValue());
                }
                jsonObject.put("" + itr + "", (Object)docJSon);
            }
            final String docType = (String)possibleDocType.get(0).get("docType");
            System.out.println("Possible Doc Type: " + jsonObject.toString());
            final List<Map<String, Object>> possibleDelims = dataPreview.analyze(ReaderType.FileReader, fqPath, ParserType.valueOf(docType));
            final JSONObject jsonObject2 = new JSONObject();
            for (int itr2 = 0; itr2 < possibleDelims.size(); ++itr2) {
                final JSONObject docJSon2 = new JSONObject();
                for (final Map.Entry<String, Object> entrySet2 : possibleDelims.get(itr2).entrySet()) {
                    docJSon2.put((String)entrySet2.getKey(), entrySet2.getValue());
                }
                jsonObject2.put("" + itr2 + "", (Object)docJSon2);
            }
            System.out.println("Possible Delimiters: " + jsonObject2.toString());
            final Map<String, Object> parserProperties = possibleDelims.get(0);
            final List<Record> data = dataPreview.doPreview(ReaderType.FileReader, fqPath, parserProperties);
            final JSONArray jArray2 = new JSONArray();
            for (final Record Record2 : data) {
                jArray2.put((Object)Record2.toJSON());
            }
            System.out.println("Data Previewd: " + jArray2.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    static {
        DataPreviewImpl.logger = Logger.getLogger((Class)DataPreviewImpl.class);
        DataPreviewImpl.MODULE_NAME = "handler";
    }
}
