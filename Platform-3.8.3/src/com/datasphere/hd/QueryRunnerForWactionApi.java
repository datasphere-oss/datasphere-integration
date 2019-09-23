package com.datasphere.hd;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.security.*;
import com.datasphere.runtime.utils.*;

import java.util.*;

import com.datasphere.distribution.*;
import com.datasphere.metaRepository.*;

public class QueryRunnerForHDApi
{
    private static Logger logger;
    private String dataStore;
    private String[] fields;
    private Map<String, Object> filters;
    private AuthToken token;
    private QueryValidator queryValidator;
    public volatile boolean gotResults;
    public ITaskEvent TE;
    MetaInfo.Query adhocQuery;
    HDApiRestCompiler compiler;
    private String keyField;
    private String[] implicitFields;
    private boolean getEventList;
    private String queryString;
    private Map<String, Object> queryParams;
    
    public QueryRunnerForHDApi() {
        this.gotResults = false;
        this.implicitFields = new String[] { "$timestamp", "$id" };
        this.getEventList = false;
    }
    
    public void setAllFields(final String dataStore, final String[] fields, final Map<String, Object> filters, final AuthToken token, final String keyField, final boolean getEventList, final QueryValidator queryValidator) {
        this.dataStore = dataStore;
        this.fields = fields.clone();
        this.filters = filters;
        this.token = token;
        this.keyField = keyField;
        this.getEventList = getEventList;
        this.queryValidator = queryValidator;
        this.compiler = new HDApiRestCompiler();
    }
    
    public void setQueryString(final String queryString, final Map<String, Object> queryParams, final QueryValidator qVal, final AuthToken token) {
        this.token = token;
        this.queryParams = queryParams;
        this.queryString = queryString;
        this.queryValidator = qVal;
    }
    
    public ITaskEvent call() throws Exception {
        this.queryValidator.createNewContext(this.token);
        this.queryValidator.setUpdateMode(true);
        String selectString;
        if (this.queryString != null) {
            selectString = this.queryString;
        }
        else if (this.filters != null && this.filters.containsKey("singlehds") && ((String)this.filters.get("singlehds")).equalsIgnoreCase("false")) {
            selectString = this.compiler.createQueryToGetLatestPerKey(this.dataStore, this.fields, this.filters, this.keyField, this.implicitFields, this.getEventList);
        }
        else {
            selectString = this.compiler.createSelectString(this.dataStore, this.fields, this.filters, this.keyField, this.implicitFields, this.getEventList);
        }
        if (selectString != null) {
            if (!selectString.endsWith(";")) {
                selectString = selectString.concat(";");
            }
            if (QueryRunnerForHDApi.logger.isInfoEnabled()) {
                QueryRunnerForHDApi.logger.info((Object)("Generated CQ: " + selectString));
                QueryRunnerForHDApi.logger.info((Object)("Token for adhoc: " + this.token));
            }
            if (this.queryParams != null) {
                final String userName = HSecurityManager.getAutheticatedUserName(this.token);
                final MetaInfo.User currentUser = HSecurityManager.get().getUser(userName);
                final String queryName = RuntimeUtils.genRandomName("queryName");
                final String fQQueryName = currentUser.getDefaultNamespace().concat(".").concat(queryName);
                final MetaInfo.Query intermediateQuery = this.queryValidator.createParameterizedQuery(false, this.token, selectString, fQQueryName);
                this.adhocQuery = this.queryValidator.prepareQuery(this.token, intermediateQuery.getUuid(), this.queryParams);
                if (QueryRunnerForHDApi.logger.isInfoEnabled()) {
                    QueryRunnerForHDApi.logger.info((Object)("Creating Paramaetrized Query with paramters: " + this.queryParams + " and Query Name: " + fQQueryName));
                    QueryRunnerForHDApi.logger.info((Object)("Generated query after compiling parameters: " + this.adhocQuery.queryDefinition));
                }
            }
            else {
                this.adhocQuery = this.queryValidator.createAdhocQuery(this.token, selectString);
            }
            if (QueryRunnerForHDApi.logger.isDebugEnabled()) {
                QueryRunnerForHDApi.logger.debug((Object)("CREATED Adhoc" + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.adhocQuery.getUuid(), this.token)));
            }
            final HQueue queue = HQueue.getQueue("consoleQueue" + this.token);
            final HQueue.Listener listener = new HQueue.Listener() {
                @Override
                public void onItem(final Object item) {
                    QueryRunnerForHDApi.this.TE = (ITaskEvent)item;
                    QueryRunnerForHDApi.this.gotResults = true;
                }
            };
            queue.subscribe(listener);
            this.queryValidator.startAdhocQuery(this.token, this.adhocQuery.getUuid());
            while (!this.gotResults) {
                Thread.sleep(10L);
            }
            this.gotResults = false;
            queue.unsubscribe(listener);
            if (QueryRunnerForHDApi.logger.isDebugEnabled()) {
                QueryRunnerForHDApi.logger.debug((Object)("In Query Runner for HD API, batch size: " + this.TE.batch().size()));
                QueryRunnerForHDApi.logger.debug((Object)("In Query Runner for HD API, batch data: " + this.TE.batch()));
            }
            return this.TE;
        }
        QueryRunnerForHDApi.logger.error((Object)("Unable to produce a valid Query using: \nName of Store: " + this.dataStore + "\nFields: " + Arrays.asList(this.fields) + "\nFilters: " + this.filters + "\nToken: " + this.token));
        throw new NullPointerException("Unable to produce a valid Query using: \nName of Store: " + this.dataStore + "\nFields: " + Arrays.asList(this.fields) + "\nFilters: " + this.filters + "\nToken: " + this.token);
    }
    
    public void cleanUp() throws MetaDataRepositoryException {
        this.queryValidator.stopAdhocQuery(this.token, this.adhocQuery.getUuid());
        this.queryValidator.deleteAdhocQuery(this.token, this.adhocQuery.getUuid());
        this.clearAllFields();
    }
    
    private void clearAllFields() {
        this.dataStore = null;
        this.fields = null;
        this.filters = null;
        this.token = null;
        this.queryString = null;
        this.queryValidator = null;
    }
    
    static {
        QueryRunnerForHDApi.logger = Logger.getLogger((Class)QueryRunnerForHDApi.class);
    }
}
