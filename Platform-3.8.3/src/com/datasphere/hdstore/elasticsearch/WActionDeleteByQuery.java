package com.datasphere.hdstore.elasticsearch;

import org.apache.log4j.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.exceptions.*;
import com.datasphere.hd.*;
import org.elasticsearch.index.reindex.*;
import com.datasphere.hdstore.*;
import java.util.*;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.*;

public class HDDeleteByQuery extends HDQuery
{
    private static Logger logger;
    
    HDDeleteByQuery(final JsonNode queryJson, final HDStore... hdStores) throws CapabilityException {
        super(queryJson, "delete", hdStores);
    }
    
    @Override
    public HDQueryResult execute() throws CapabilityException {
        final HDStore hdStore = this.getHDStore(0);
        final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
        final Client client = manager.getClient();
        final JsonNode whereClause = this.getQuery().get("where");
        QueryBuilder queryBuilder = (QueryBuilder)QueryBuilders.matchAllQuery();
        if (whereClause != null) {
            final QueryBuilder filter = HDQuery.getLogicalOperation(whereClause);
            queryBuilder = (QueryBuilder)QueryBuilders.boolQuery().must(queryBuilder).filter(filter);
        }
        if (HDDeleteByQuery.logger.isDebugEnabled()) {
            HDDeleteByQuery.logger.debug((Object)("Executing query " + queryBuilder));
        }
        final BulkByScrollResponse response = (BulkByScrollResponse)((DeleteByQueryRequestBuilder)((DeleteByQueryRequestBuilder)DeleteByQueryAction.INSTANCE.newRequestBuilder((ElasticsearchClient)client).filter(queryBuilder)).source(new String[] { hdStore.getActualName() })).get();
        if (HDDeleteByQuery.logger.isDebugEnabled() && response.getDeleted() > 0L) {
            HDDeleteByQuery.logger.debug((Object)("Deleted " + response.getDeleted() + " hds"));
        }
        final HD result = new HD();
        result.put("Result", 0);
        return new HDQueryResult() {
            @Override
            public Iterator<HD> iterator() {
                final HD[] hds = { result };
                return Arrays.asList(hds).iterator();
            }
        };
    }
    
    static {
        HDDeleteByQuery.logger = Logger.getLogger((Class)HDDeleteByQuery.class);
    }
}
