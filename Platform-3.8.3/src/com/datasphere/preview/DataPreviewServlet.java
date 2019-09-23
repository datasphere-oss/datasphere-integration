package com.datasphere.preview;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.exception.SecurityException;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.records.Record;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;

public class DataPreviewServlet extends HttpServlet
{
    private static Logger logger;
    DataPreview dataPreview;
    SampleDataFiles sampleDataFiles;
    QueryValidator queryValidator;
    MDRepository mdRepository;
    
    public DataPreviewServlet() {
        this.queryValidator = new QueryValidator(Server.server);
        this.mdRepository = MetadataRepository.getINSTANCE();
    }
    
    public void init() throws ServletException {
        super.init();
        this.dataPreview = new DataPreviewImpl();
        this.sampleDataFiles = new SampleDataFiles();
    }
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException {
        int responseStatus = 200;
        String responseData = null;
        final String error = null;
        try {
            final String fqPathToFile = request.getParameter(ResultWordsForGetRequest.path.name());
            if (fqPathToFile == null) {
                throw new NullPointerException("Path to Data File is NULL");
            }
            if (fqPathToFile.equalsIgnoreCase("samples")) {
                if (!this.sampleDataFiles.samplesListCreated) {
                    this.sampleDataFiles.init(DataPreviewConstants.HOME);
                }
                final List<DataFile> samples = this.sampleDataFiles.getSamples();
                final JSONObject jsonObject = new JSONObject();
                final JSONArray jArray = new JSONArray();
                final Iterator<DataFile> dataFileIterator = samples.iterator();
                while (dataFileIterator.hasNext()) {
                    jArray.put((Object)dataFileIterator.next().toJson());
                }
                jsonObject.put("samples", (Object)jArray);
                responseData = jsonObject.toString();
            }
            else {
                final String readerType = request.getParameter(ResultWordsForGetRequest.readertype.name());
                final String docType = request.getParameter(ResultWordsForGetRequest.doctype.name());
                final String[] parserProperties = request.getQueryString().split("parserProperties=");
                if (DataPreviewServlet.logger.isDebugEnabled()) {
                    DataPreviewServlet.logger.debug((Object)("Get Request Params\nFully Qualified Path To File: " + fqPathToFile + "\n Reader Type: " + readerType + "\nDoc Type: " + docType + "\n Parser Properties Length: " + parserProperties.length));
                }
                final ReaderType fType = ReaderType.valueOf(readerType);
                Map<String, Object> parserPropertyMap = null;
                ParserType pType;
                if (docType == null || docType.isEmpty()) {
                    pType = null;
                }
                else {
                    pType = ParserType.valueOf(docType);
                }
                if (parserProperties.length >= 2) {
                    if (parserProperties[1] != null) {
                        final String[] splitByComma = parserProperties[1].split("\\|");
                        if (splitByComma.length > 0) {
                            for (final String ss : splitByComma) {
                                final String[] splitByColon = ss.split(":");
                                if (parserPropertyMap == null) {
                                    parserPropertyMap = new HashMap<String, Object>();
                                }
                                if (splitByColon.length > 1) {
                                    parserPropertyMap.put(splitByColon[0], URLDecoder.decode(splitByColon[1], "UTF-8"));
                                }
                                else {
                                    parserPropertyMap.put(splitByColon[0], null);
                                }
                            }
                        }
                    }
                }
                if (DataPreviewServlet.logger.isDebugEnabled() && parserPropertyMap != null) {
                    DataPreviewServlet.logger.debug((Object)("Parser Properties: " + parserPropertyMap.entrySet()));
                }
                final JSONObject jsonObject2 = new JSONObject();
                if (parserPropertyMap != null && !parserPropertyMap.isEmpty()) {
                    parserPropertyMap.put(DataPreviewImpl.MODULE_NAME, pType.getForClassName());
                    final List<Record> resultOfPreview = this.dataPreview.doPreview(fType, fqPathToFile, parserPropertyMap);
                    final JSONArray jArray2 = new JSONArray();
                    for (final Record Record : resultOfPreview) {
                        jArray2.put((Object)Record.getJSONObject());
                    }
                    jsonObject2.put(ResultWordsForGetRequest.events.name(), (Object)jArray2);
                    responseData = jsonObject2.toString();
                    if (DataPreviewServlet.logger.isDebugEnabled()) {
                        DataPreviewServlet.logger.debug((Object)("Response Data: " + responseData));
                    }
                }
                else {
                    final List<Record> rawData = this.dataPreview.getRawData(fqPathToFile, fType);
                    final JSONArray jArray3 = new JSONArray();
                    for (final Record Record : rawData) {
                        jArray3.put((Object)Record.getJSONObject());
                    }
                    jsonObject2.put(ResultWordsForGetRequest.events.name(), (Object)jArray3);
                    if (DataPreviewServlet.logger.isDebugEnabled()) {
                        DataPreviewServlet.logger.debug((Object)("Raw events: " + jsonObject2.get(ResultWordsForGetRequest.events.name())));
                    }
                    final DataFile fileInfo = this.dataPreview.getFileInfo(fqPathToFile, fType);
                    jsonObject2.put(ResultWordsForGetRequest.fileinfo.name(), (Object)fileInfo.toJson());
                    if (DataPreviewServlet.logger.isDebugEnabled()) {
                        DataPreviewServlet.logger.debug((Object)("File Info: " + jsonObject2.get(ResultWordsForGetRequest.fileinfo.name())));
                    }
                    final List<Map<String, Object>> resultOfAnanlysis = this.dataPreview.analyze(fType, fqPathToFile, pType);
                    final JSONObject jObject = new JSONObject();
                    String recommendedDocType;
                    if (pType == null) {
                        recommendedDocType = (String)resultOfAnanlysis.get(0).get("docType");
                        jsonObject2.put(ResultWordsForGetRequest.doctype.name(), (Object)recommendedDocType);
                        if (DataPreviewServlet.logger.isDebugEnabled()) {
                            DataPreviewServlet.logger.debug((Object)("Recommending parser: " + jsonObject2.get(ResultWordsForGetRequest.doctype.name())));
                        }
                    }
                    else {
                        recommendedDocType = pType.name();
                        for (int itr = 0; itr < resultOfAnanlysis.size(); ++itr) {
                            final JSONObject docJSon = new JSONObject();
                            for (final Map.Entry<String, Object> entrySet : resultOfAnanlysis.get(itr).entrySet()) {
                                docJSon.put((String)entrySet.getKey(), entrySet.getValue());
                            }
                            jObject.put("" + itr + "", (Object)docJSon);
                        }
                        jsonObject2.put(ResultWordsForGetRequest.recommendation.name(), (Object)jObject);
                        if (DataPreviewServlet.logger.isDebugEnabled()) {
                            DataPreviewServlet.logger.debug((Object)("Recommending delimiters: " + jsonObject2.get(ResultWordsForGetRequest.recommendation.name())));
                        }
                    }
                    final MetaInfo.PropertyTemplateInfo propertyTemplateInfo = this.dataPreview.getPropertyTemplate(ParserType.valueOf(recommendedDocType).getParserName());
                    final JSONObject pJsonObject = new JSONObject();
                    for (final Map.Entry<String, MetaInfo.PropertyDef> entry : propertyTemplateInfo.getPropertyMap().entrySet()) {
                        final MetaInfo.PropertyDef propertyDef = entry.getValue();
                        final JSONObject pDefJsonObject = new JSONObject();
                        pDefJsonObject.put(PropertyDefFields.Required.name(), propertyDef.isRequired());
                        pDefJsonObject.put(PropertyDefFields.DefaultValue.name(), (Object)propertyDef.getDefaultValue());
                        pDefJsonObject.put(PropertyDefFields.Type.name(), (Object)propertyDef.getType().getSimpleName());
                        pJsonObject.put((String)entry.getKey(), (Object)pDefJsonObject);
                    }
                    jsonObject2.put(ResultWordsForGetRequest.proptemplate.name(), (Object)pJsonObject);
                    responseData = jsonObject2.toString();
                }
            }
        }
        catch (NullPointerException | MetaDataRepositoryException | JSONException | IOException ex2) {
            DataPreviewServlet.logger.error((Object)"Problem performing request ", (Throwable)ex2);
            responseStatus = 400;
            responseData = ex2.getMessage();
        }
        catch (Exception e) {
            DataPreviewServlet.logger.error((Object)"Problem performing request ", (Throwable)e);
            responseStatus = 500;
            responseData = e.getMessage();
        }
        response.setStatus(responseStatus);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        try {
            if (responseData != null) {
                response.getWriter().write(responseData);
            }
            else {
                response.getWriter().write(error);
            }
        }
        catch (IOException e2) {
            DataPreviewServlet.logger.error((Object)"Problem sending response", (Throwable)e2);
        }
    }
    
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        int responseStatus = 200;
        String responseData = null;
        final String error = null;
        AuthToken authToken = null;
        final String requestAsString = request.getParameter(ResultWordsForPostRequest.model.name());
        try {
            final JSONObject requestAsJson = new JSONObject(requestAsString);
            String token = null;
            if (requestAsJson.has(ResultWordsForPostRequest.token.name())) {
                token = requestAsJson.getString("token");
            }
            if (token == null) {
                throw new SecurityException("Yiikkkeeeessss..Authentication Token cannot be NULL");
            }
            String pathToFile = null;
            if (!requestAsJson.has(ResultWordsForGetRequest.path.name())) {
                throw new NullPointerException("Path to data file cannot be NULL");
            }
            pathToFile = requestAsJson.getString(ResultWordsForGetRequest.path.name());
            String columns = null;
            if (!requestAsJson.has(ResultWordsForPostRequest.columns.name())) {
                throw new NullPointerException("Column data can't be NULL");
            }
            columns = requestAsJson.getString(ResultWordsForPostRequest.columns.name());
            String parserType = null;
            if (!requestAsJson.has(ResultWordsForGetRequest.doctype.name())) {
                throw new NullPointerException("Document Type can't be NULL");
            }
            parserType = requestAsJson.getString(ResultWordsForGetRequest.doctype.name());
            String readerType = null;
            if (!requestAsJson.has(ResultWordsForGetRequest.readertype.name())) {
                throw new NullPointerException("Reader Type can't be NULL");
            }
            readerType = requestAsJson.getString(ResultWordsForGetRequest.readertype.name());
            String parserString = null;
            if (!requestAsJson.has(ResultWordsForGetRequest.parserproperties.name())) {
                throw new NullPointerException("Parser Properties can't be NULL");
            }
            parserString = requestAsJson.getString(ResultWordsForGetRequest.parserproperties.name());
            String namespaceForCurrentOperation = null;
            if (requestAsJson.has(ResultWordsForPostRequest.namespace.name())) {
                namespaceForCurrentOperation = requestAsJson.getString(ResultWordsForPostRequest.namespace.name());
            }
            String appName = null;
            if (requestAsJson.has(ResultWordsForPostRequest.appname.name())) {
                appName = requestAsJson.getString(ResultWordsForPostRequest.appname.name());
            }
            String cacheName = null;
            if (requestAsJson.has(ResultWordsForPostRequest.cachename.name())) {
                cacheName = requestAsJson.getString(ResultWordsForPostRequest.cachename.name());
            }
            String cacheOptionsString = null;
            if (requestAsJson.has(ResultWordsForPostRequest.cacheproperties.name())) {
                cacheOptionsString = requestAsJson.getString(ResultWordsForPostRequest.cacheproperties.name());
            }
            boolean loadCache = false;
            if (requestAsJson.has(ResultWordsForPostRequest.loaddata.name())) {
                loadCache = requestAsJson.getBoolean(ResultWordsForPostRequest.loaddata.name());
            }
            String sourceName = null;
            if (requestAsJson.has(ResultWordsForPostRequest.sourcename.name())) {
                sourceName = requestAsJson.getString(ResultWordsForPostRequest.sourcename.name());
            }
            this.makeSureSourceOrCacheNameisSet(cacheName, sourceName);
            final List<DataTypePosition> dataPositions = new ArrayList<DataTypePosition>();
            String typeName = null;
            List<Map<String, String>> readerProperties = null;
            List<Map<String, String>> parserProperties = null;
            if (DataPreviewServlet.logger.isDebugEnabled()) {
                DataPreviewServlet.logger.debug((Object)("POST Reuqest Params\n Token: " + token + "\n Path To File: " + pathToFile + "\n App Name: " + appName + " Columns: " + columns + "\n Doc Type: " + parserType + "\n Reader Type: " + readerType + "\n Parser Props: " + parserString + "\n Cache Name: " + cacheName + "\n Cache Options: " + cacheOptionsString + "\n Source Name: " + sourceName));
            }
            if (token != null) {
                authToken = new AuthToken(token);
            }
            this.setupQueryValidator(authToken);
            if (namespaceForCurrentOperation == null) {
                namespaceForCurrentOperation = this.getCurrentNamespace(authToken);
            }
            else {
                final MetaInfo.MetaObject namespace = this.mdRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespaceForCurrentOperation, null, authToken);
                if (namespace == null || namespace.getMetaInfoStatus().isDropped()) {
                    this.queryValidator.CreateNameSpaceStatement(authToken, namespaceForCurrentOperation, false);
                }
            }
            if (namespaceForCurrentOperation == null) {
                throw new NullPointerException("Current User does not have a default Namespace");
            }
            this.setCurrentNamespace(namespaceForCurrentOperation, authToken);
            MetaInfo.MetaObject app = this.mdRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceForCurrentOperation, appName, null, authToken);
            if (app == null || app.getMetaInfoStatus().isDropped()) {
                this.queryValidator.CreateAppStatement(authToken, appName, false, null, null, null, null, null, null);
                app = this.mdRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceForCurrentOperation, appName, null, authToken);
            }
            this.queryValidator.setCurrentApp(app.getFullName(), authToken);
            if (columns == null) {
                throw new NullPointerException("Columns sent from the browser are null, therefore we can't create a Type definition.");
            }
            ReaderType reader = null;
            ParserType parser = null;
            if (readerType == null) {
                throw new NullPointerException("Reader Type not specified");
            }
            reader = ReaderType.valueOf(readerType);
            if (parserType == null) {
                throw new NullPointerException("Parser Type not sepcified");
            }
            parser = ParserType.valueOf(parserType);
            if (pathToFile != null) {
                readerProperties = new ArrayList<Map<String, String>>();
                this.putPathInReaderProperties(readerProperties, reader, pathToFile);
            }
            if (parserString != null) {
                parserProperties = new ArrayList<Map<String, String>>();
                this.createParserProperties(parserProperties, parserString);
            }
            this.addToReaderPropertiesList(readerProperties, reader.getDefaultReaderPropertiesAsString());
            if (sourceName != null) {
                typeName = sourceName.concat("_Type");
                this.createAndStoreType(columns, dataPositions, namespaceForCurrentOperation, typeName, authToken, false);
                final String destinationstreamName = sourceName.concat("_TransformedStream");
                final String implicitSouceStreamName = sourceName.concat("_Stream");
                if (authToken == null || sourceName == null || readerProperties == null || readerProperties.isEmpty() || parserProperties == null || parserProperties.isEmpty() || implicitSouceStreamName == null) {
                    if (DataPreviewServlet.logger.isDebugEnabled()) {
                        DataPreviewServlet.logger.debug((Object)("Unexpected NULL value[authToken: " + authToken + ", sourceName: " + sourceName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " streamName: " + implicitSouceStreamName + "]"));
                    }
                    throw new NullPointerException("Unexpected NULL value[authToken: " + authToken + ", sourceName: " + sourceName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " streamName: " + implicitSouceStreamName + "]");
                }
                this.createAndStoreSource(authToken, sourceName, false, reader.name(), readerProperties, parser.getParserName(), parserProperties, implicitSouceStreamName, namespaceForCurrentOperation);
                final String implicitCqName = sourceName.concat("_CQ");
                this.createAndStoreCq(authToken, namespaceForCurrentOperation, implicitCqName, false, implicitSouceStreamName, destinationstreamName, new String[0], dataPositions);
            }
            if (cacheName != null && cacheOptionsString != null) {
                final MetaInfo.MetaObject cache = this.mdRepository.getMetaObjectByName(EntityType.CACHE, namespaceForCurrentOperation, cacheName, null, authToken);
                if (cache != null && !cache.getMetaInfoStatus().isDropped()) {
                    throw new MetaDataRepositoryException("The cache name " + cacheName + " already exists in namespace " + namespaceForCurrentOperation);
                }
                typeName = cacheName.concat("_Type");
                this.createAndStoreType(columns, dataPositions, namespaceForCurrentOperation, typeName, authToken, true);
                if (authToken == null || cacheName == null || readerProperties == null || readerProperties.isEmpty() || parserProperties == null || parserProperties.isEmpty() || !(cacheOptionsString != null & typeName != null)) {
                    if (DataPreviewServlet.logger.isDebugEnabled()) {
                        DataPreviewServlet.logger.debug((Object)("Unexpected NULL value[authToken: " + authToken + ", cacheName: " + cacheName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " cacheOptionsString: " + cacheOptionsString + ", Type Name: " + typeName + "]"));
                    }
                    throw new NullPointerException("Unexpected NULL value[authToken: " + authToken + ", cacheName: " + cacheName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " cacheOptionsString: " + cacheOptionsString + ", Type Name: " + typeName + "]");
                }
                this.createAndStoreCache(authToken, appName, cacheName, false, reader.name(), readerProperties, parser.getParserName(), parserProperties, cacheOptionsString, typeName, loadCache, namespaceForCurrentOperation);
            }
            this.queryValidator.setCurrentApp(null, authToken);
            responseData = "Success";
        }
        catch (SecurityException | NullPointerException | MetaDataRepositoryException | JSONException ex2) {
            DataPreviewServlet.logger.error((Object)("Problem saving results of source preview from request: " + requestAsString), (Throwable)ex2);
            if (ex2 instanceof SecurityException) {
                responseStatus = 401;
            }
            else {
                responseStatus = 400;
            }
            responseData = ex2.getMessage();
        }
        catch (Exception e) {
            DataPreviewServlet.logger.error((Object)("Problem saving results of source preview from request: " + requestAsString), (Throwable)e);
            responseStatus = 400;
            responseData = e.getMessage();
        }
        this.queryValidator.removeContext(authToken);
        response.setStatus(responseStatus);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        try {
            if (responseData != null) {
                response.getWriter().write(responseData);
            }
            else {
                response.getWriter().write(error);
            }
        }
        catch (IOException e2) {
            DataPreviewServlet.logger.error((Object)e2.getMessage());
        }
    }
    
    private void makeSureSourceOrCacheNameisSet(final String cacheName, final String sourceName) throws NullPointerException {
        if (cacheName == null && sourceName == null) {
            throw new NullPointerException("Both the Source name and Cache name are NULL, please save the configuration either as a Real-time Source or as a Cache");
        }
    }
    
    private List<Map<String, String>> makeCacheProperties(final String cacheOptionsString) throws JSONException {
        final JSONObject json = new JSONObject(cacheOptionsString);
        final Iterator iter = json.keys();
        final List<Map<String, String>> properties = new ArrayList<Map<String, String>>();
        while (iter.hasNext()) {
            final Map<String, String> property = new HashMap<String, String>();
            final String key = (String)iter.next();
            property.put(key, json.getString(key));
            properties.add(property);
        }
        return properties;
    }
    
    private void createAndStoreCache(final AuthToken token, final String appName, final String cacheName, final Boolean r, final String reader_type, final List<Map<String, String>> reader_props, final String parser_type, final List<Map<String, String>> parser_props, final String cacheOptionsString, final String typename, final boolean loadCache, final String namespaceForCurrentOperation) throws MetaDataRepositoryException, JSONException {
        final MetaInfo.MetaObject cache = this.mdRepository.getMetaObjectByName(EntityType.CACHE, namespaceForCurrentOperation, cacheName, null, token);
        if (cache == null) {
            final List<Map<String, String>> cacheOptions = this.makeCacheProperties(cacheOptionsString);
            final List<Property> readerPropList = new ArrayList<Property>();
            for (final Map<String, String> map : reader_props) {
                final Map.Entry<String, String> singleEntry = map.entrySet().iterator().next();
                readerPropList.add(new Property(singleEntry.getKey(), singleEntry.getValue()));
            }
            final List<Property> parserPropList = new ArrayList<Property>();
            for (final Map<String, String> map2 : parser_props) {
                final Map.Entry<String, String> singleEntry2 = map2.entrySet().iterator().next();
                parserPropList.add(new Property(singleEntry2.getKey(), singleEntry2.getValue()));
            }
            final List<Property> cachePropList = new ArrayList<Property>();
            for (final Map<String, String> map3 : cacheOptions) {
                final Map.Entry<String, String> singleEntry3 = map3.entrySet().iterator().next();
                cachePropList.add(new Property(singleEntry3.getKey(), singleEntry3.getValue()));
            }
            this.queryValidator.CreateCacheStatement_New(token, cacheName, r, reader_type, readerPropList, parser_type, parserPropList, cachePropList, typename);
            if (loadCache) {
                final Map<String, String> appDescription = new HashMap<String, String>();
                appDescription.put("flow", appName);
                appDescription.put("strategy", "any");
                this.queryValidator.CreateDeployFlowStatement(token, EntityType.APPLICATION.name(), appDescription, null);
            }
            return;
        }
        throw new MetaDataRepositoryException("The cache name " + cacheName + " already exists in namespace " + namespaceForCurrentOperation);
    }
    
    public void addToReaderPropertiesList(final List<Map<String, String>> readerPropertiesList, final Map<String, String> readerProperties) {
        for (final Map.Entry<String, String> entry : readerProperties.entrySet()) {
            final Map<String, String> property = new HashMap<String, String>();
            property.put(entry.getKey(), entry.getValue());
            readerPropertiesList.add(property);
        }
    }
    
    private String getCurrentNamespace(final AuthToken authToken) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.User user = HSecurityManager.get().getAuthenticatedUser(authToken);
        if (user != null) {
            return user.getDefaultNamespace();
        }
        return null;
    }
    
    private void setCurrentNamespace(final String namespaceForCurrentOperation, final AuthToken authToken) throws MetaDataRepositoryException {
        this.queryValidator.CreateUseSchemaStmt(authToken, namespaceForCurrentOperation);
    }
    
    private void createAndStoreCq(final AuthToken token, final String namespaceForCurrentOperation, final String cq_name, final Boolean doReplace, final String implicitSouceStreamName, final String destinationstreamName, final String[] field_name_list, final List<DataTypePosition> dPosition) throws Exception {
        final MetaInfo.MetaObject cq = this.mdRepository.getMetaObjectByName(EntityType.CQ, namespaceForCurrentOperation, cq_name, null, token);
        if (cq == null || cq.getMetaInfoStatus().isDropped()) {
            String selectText = "";
            selectText += ResultWordsForPostRequest.SELECT.name();
            selectText += " ";
            int size = dPosition.size();
            for (final DataTypePosition dPos : dPosition) {
                final String type = dPos.type;
                switch (type) {
                    case "String": {
                        selectText += DataTypesForCq.String.getFunction();
                        break;
                    }
                    case "Integer": {
                        selectText += DataTypesForCq.Integer.getFunction();
                        break;
                    }
                    case "Long": {
                        selectText += DataTypesForCq.Long.getFunction();
                        break;
                    }
                    case "DateTime": {
                        selectText += DataTypesForCq.Date.getFunction();
                        break;
                    }
                    case "Float": {
                        selectText += DataTypesForCq.Float.getFunction();
                        break;
                    }
                    case "Double": {
                        selectText += DataTypesForCq.Double.getFunction();
                        break;
                    }
                    case "Boolean": {
                        selectText += DataTypesForCq.Boolean.getFunction();
                        break;
                    }
                    case "Short": {
                        selectText += DataTypesForCq.Short.getFunction();
                        break;
                    }
                }
                selectText = selectText + "(data[" + dPos.position + "]) as " + dPos.name;
                if (size > 1) {
                    selectText += ",\n  ";
                }
                else {
                    selectText += "\n";
                }
                --size;
            }
            selectText += ResultWordsForPostRequest.FROM.name();
            selectText += " ";
            selectText += implicitSouceStreamName;
            selectText += ";";
            try {
                this.queryValidator.CreateCqStatement(token, cq_name, doReplace, destinationstreamName, field_name_list, selectText, null);
            }
            catch (Throwable t) {
                DataPreviewServlet.logger.error((Object)("Problem creating CQ from: " + selectText), t);
                throw t;
            }
            return;
        }
        throw new MetaDataRepositoryException("CQ with name: " + cq_name + " already exists");
    }
    
    private void createAndStoreSource(final AuthToken authToken, final String sourceName, final Boolean b, final String readerName, final List<Map<String, String>> readerProperties, final String parserName, final List<Map<String, String>> parserProperties, final String implicitStreamName, final String namespaceForCurrentOperation) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject stream = this.mdRepository.getMetaObjectByName(EntityType.STREAM, namespaceForCurrentOperation, implicitStreamName, null, authToken);
        if (stream != null && !stream.getMetaInfoStatus().isDropped()) {
            throw new MetaDataRepositoryException("The source name " + sourceName + " already exists in namespace " + namespaceForCurrentOperation);
        }
        final MetaInfo.MetaObject source = this.mdRepository.getMetaObjectByName(EntityType.SOURCE, namespaceForCurrentOperation, sourceName, null, authToken);
        if (source == null || source.getMetaInfoStatus().isDropped()) {
            final List<Property> readerPropList = new ArrayList<Property>();
            for (final Map<String, String> map : readerProperties) {
                final Map.Entry<String, String> singleEntry = map.entrySet().iterator().next();
                readerPropList.add(new Property(singleEntry.getKey(), singleEntry.getValue()));
            }
            final List<Property> parserPropList = new ArrayList<Property>();
            for (final Map<String, String> map2 : parserProperties) {
                final Map.Entry<String, String> singleEntry2 = map2.entrySet().iterator().next();
                parserPropList.add(new Property(singleEntry2.getKey(), singleEntry2.getValue()));
            }
            this.queryValidator.CreateSourceStatement_New(authToken, sourceName, b, readerName, null, readerPropList, parserName, null, parserPropList, implicitStreamName);
            return;
        }
        throw new MetaDataRepositoryException("The source name " + sourceName + " already exists in namespace " + namespaceForCurrentOperation);
    }
    
    private void createParserProperties(final List<Map<String, String>> parserProperties, final String parserString) throws JSONException {
        final JSONObject jParser = new JSONObject(parserString);
        final Iterator allKeys = jParser.keys();
        while (allKeys.hasNext()) {
            final Map<String, String> property = new HashMap<String, String>();
            final String key = (String)allKeys.next();
            property.put(key, jParser.getString(key));
            parserProperties.add(property);
        }
    }
    
    public void putPathInReaderProperties(final List<Map<String, String>> readerProperties, final ReaderType reader, final String pathToFile) throws Exception {
        final int pos = pathToFile.lastIndexOf("/");
        final String dir = pathToFile.substring(0, pos + 1);
        final String fileName = pathToFile.substring(pos + 1, pathToFile.length());
        if (fileName == null) {
            throw new Exception("File Name is NULL");
        }
        if (dir == null) {
            throw new Exception("Directory is NULL");
        }
        Map<String, String> property = null;
        property = new HashMap<String, String>();
        switch (reader) {
            case FileReader: {
                property.put(ResultWordsForPostRequest.directory.name(), dir);
                break;
            }
            case HDFSReader: {
                property.put(ResultWordsForPostRequest.hadoopurl.name(), dir);
                break;
            }
            default: {
                throw new Exception("Unexpected Reader Type, allowed: " + Arrays.asList(ReaderType.values()) + ", sent " + reader);
            }
        }
        readerProperties.add(property);
        property = new HashMap<String, String>();
        property.put(ResultWordsForPostRequest.wildcard.name(), fileName);
        readerProperties.add(property);
    }
    
    public AuthToken setupQueryValidator(final AuthToken authToken) throws Exception {
        this.queryValidator.createNewContext(authToken);
        this.queryValidator.setUpdateMode(true);
        return authToken;
    }
    
    private void createAndStoreType(final String columns, final List<DataTypePosition> dataPositions, final String namespaceForCurrentOperation, final String typeName, final AuthToken authToken, final boolean doStore) throws Exception {
        final MetaInfo.MetaObject typeExists = this.mdRepository.getMetaObjectByName(EntityType.TYPE, namespaceForCurrentOperation, typeName, null, authToken);
        if (typeExists == null || typeExists.getMetaInfoStatus().isDropped()) {
            final JSONArray jsonArray = new JSONArray(columns);
            final int size = jsonArray.length();
            final List<Map<String, String>> typeDef = new ArrayList<Map<String, String>>();
            for (int position = 0; position < size; ++position) {
                final JSONObject colDef = (JSONObject)jsonArray.get(position);
                final Map<String, String> fieldDef = new HashMap<String, String>();
                final boolean isDiscard = colDef.getBoolean("discard");
                if (!isDiscard) {
                    final String fieldName = colDef.getString("name");
                    final String fieldType = colDef.getString("type");
                    final DataTypePosition dPos = new DataTypePosition(position, fieldType, fieldName);
                    dataPositions.add(dPos);
                    fieldDef.put(fieldName, fieldType);
                    typeDef.add(fieldDef);
                }
            }
            final TypeField[] typeDefArray = typeDef.toArray(new TypeField[typeDef.size()]);
            if (DataPreviewServlet.logger.isDebugEnabled()) {
                DataPreviewServlet.logger.debug((Object)("Storing Type\n AuthToken: " + authToken + "\n TypeName: " + typeName + "\n Type Definition: " + typeDefArray));
            }
            if (doStore) {
                this.queryValidator.CreateTypeStatement_New(authToken, typeName, false, typeDefArray);
            }
            return;
        }
        throw new MetaDataRepositoryException("Type with name " + typeName + " exists");
    }
    
    static {
        DataPreviewServlet.logger = Logger.getLogger((Class)DataPreviewServlet.class);
    }
    
    public enum ResultWordsForGetRequest
    {
        path, 
        readertype, 
        events, 
        fileinfo, 
        recommendation, 
        doctype, 
        delimiters, 
        header, 
        proptemplate, 
        parserproperties;
    }
    
    public enum ResultWordsForPostRequest
    {
        model, 
        token, 
        columns, 
        directory, 
        hadoopurl, 
        wildcard, 
        sourcename, 
        streamname, 
        appname, 
        namespace, 
        cachename, 
        cacheproperties, 
        loaddata, 
        SELECT, 
        FROM;
    }
    
    public enum PropertyDefFields
    {
        Required, 
        Type, 
        DefaultValue;
    }
    
    public enum DataTypesForCq
    {
        String("TO_STRING"), 
        Integer("TO_INT"), 
        Long("TO_LONG"), 
        Date("TO_DATE"), 
        Float("TO_FLOAT"), 
        Double("TO_DOUBLE"), 
        Boolean("TO_BOOLEAN"), 
        Short("TO_SHORT");
        
        String function;
        
        private DataTypesForCq(final String function) {
            this.function = null;
            this.function = function;
        }
        
        public String getFunction() {
            return this.function;
        }
    }
    
    public class SimpleMetaObjectInfo
    {
        EntityType entityType;
        String name;
        
        public SimpleMetaObjectInfo(final EntityType entityType, final String name) {
            this.entityType = entityType;
            this.name = name;
        }
    }
    
    public class DataTypePosition
    {
        int position;
        String type;
        String name;
        
        public DataTypePosition(final int position, final String fieldType, final String name) {
            this.position = position;
            this.type = fieldType;
            this.name = name;
        }
        
        @Override
        public String toString() {
            return this.position + ":" + this.type + ":" + this.name;
        }
    }
}
