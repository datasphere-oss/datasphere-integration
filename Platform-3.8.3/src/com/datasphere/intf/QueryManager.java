package com.datasphere.intf;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.event.ObjectMapperFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.select.RSFieldDesc;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public interface QueryManager
{
    public static final Logger logger = Logger.getLogger(QueryManager.class.getName());
    
    List<MetaInfo.Query> listAllQueries(final AuthToken p0) throws MetaDataRepositoryException;
    
    List<MetaInfo.Query> listAdhocQueries(final AuthToken p0) throws MetaDataRepositoryException;
    
    List<MetaInfo.Query> listNamedQueries(final AuthToken p0) throws MetaDataRepositoryException;
    
    MetaInfo.Query createAdhocQuery(final AuthToken p0, final String p1) throws Exception;
    
    MetaInfo.Query createAdhocQueryFromJSON(final AuthToken p0, final String p1) throws Exception;
    
    MetaInfo.Query createNamedQuery(final AuthToken p0, final String p1) throws Exception;
    
    MetaInfo.Query createNamedQueryFromJSON(final AuthToken p0, final String p1) throws Exception;
    
    void deleteAdhocQuery(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    void deleteNamedQueryByUUID(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    void deleteNamedQueryByName(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
    
    MetaInfo.Query cloneNamedQueryFromAdhoc(final AuthToken p0, final UUID p1, final String p2) throws Exception;
    
    boolean startAdhocQuery(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    boolean startNamedQuery(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    boolean stopAdhocQuery(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    boolean stopNamedQuery(final AuthToken p0, final UUID p1) throws MetaDataRepositoryException;
    
    UUID createQueryIfNotExists(final AuthToken p0, final String p1, final String p2) throws Exception;
    
    MetaInfo.Query prepareQuery(final AuthToken p0, final UUID p1, final Map<String, Object> p2) throws MetaDataRepositoryException, IllegalArgumentException, Exception;
    
    MetaInfo.Query createParameterizedQuery(final boolean p0, final AuthToken p1, final String p2, final String p3) throws Exception;
    
    public static class QueryParameters
    {
        private Set<String> stringParams;
        private Set<String> otherParams;
        
        public QueryParameters() {
            this.stringParams = new HashSet<String>();
            this.otherParams = new HashSet<String>();
        }
        
        public Set<String> allParams() {
            final Set<String> fullList = new HashSet<String>();
            fullList.addAll(this.getStringParams());
            fullList.addAll(this.getOtherParams());
            return fullList;
        }
        
        public int size() {
            return this.allParams().size();
        }
        
        public Set<String> getStringParams() {
            return this.stringParams;
        }
        
        public void setStringParams(final Set<String> stringParams) {
            this.stringParams = stringParams;
        }
        
        public Set<String> getOtherParams() {
            return this.otherParams;
        }
        
        public void setOtherParams(final Set<String> otherParams) {
            this.otherParams = otherParams;
        }
        
        public void addOtherParams(final String string) {
            this.otherParams.add(string);
        }
    }
    
    public static class QueryProjection implements Serializable
    {
        private static final long serialVersionUID = 1838167613576358600L;
        public String name;
        public String type;
        
        @Override
        public String toString() {
            try {
                return this.JSONify().toString();
            }
            catch (JSONException e) {
                if (QueryManager.logger.isDebugEnabled()) {
                    QueryManager.logger.info((Object)e.getMessage(), (Throwable)e);
                }
                return "";
            }
        }
        
        public QueryProjection() {
        }
        
        public QueryProjection(final RSFieldDesc field) {
            this.name = field.name;
            this.type = ((field.type == null) ? null : field.type.getName());
        }
        
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = new JSONObject();
            json.put("name", (Object)this.name);
            json.put("type", (Object)this.type);
            return json;
        }
        
        @JsonIgnore
        public ObjectNode getJsonForClient() {
            final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
            final ObjectNode rootNode = jsonMapper.createObjectNode();
            rootNode.put("name", this.name);
            rootNode.put("type", this.type);
            return rootNode;
        }
        
        public static List<QueryProjection> jsonDeserializer(final List results) throws JsonParseException, JsonMappingException, IOException {
            if (results == null) {
                return null;
            }
            final List<QueryProjection> qps = new ArrayList<QueryProjection>();
            for (int ik = 0; ik < results.size(); ++ik) {
                final Object obj = results.get(ik);
                if (obj instanceof Map) {
                    qps.add(jsonDeserializer((Map)results.get(ik)));
                }
                else if (obj instanceof String) {
                    final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
                    jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
                    final Map map = (Map)jsonMapper.readValue((String)obj, (Class)Map.class);
                    qps.add(jsonDeserializer(map));
                }
            }
            return qps;
        }
        
        public static QueryProjection jsonDeserializer(final Map results) {
            if (results == null) {
                return null;
            }
            final QueryProjection qp = new QueryProjection();
            if (results.get("name") != null) {
                qp.name = (String)results.get("name");
            }
            if (results.get("type") != null) {
                qp.type = (String)results.get("type");
            }
            return qp;
        }
    }
    
    public enum TYPE
    {
        ADHOC, 
        NAMEDQUERY;
    }
}
