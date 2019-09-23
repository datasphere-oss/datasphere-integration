package com.datasphere.hdstore.elasticsearch;

import com.datasphere.hd.*;
import org.apache.log4j.*;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.*;
import org.elasticsearch.action.search.*;
import java.util.*;
import com.datasphere.hdstore.*;

public class ElasticSearchQueryResult extends HDQueryResult
{
    private static final Logger logger;
    private SearchRequestBuilder searchRequest;
    private static final int scrollSize;
    private boolean containsAggregations;
    private static Client client;
    private static final TimeValue scrollTimeout;
    
    ElasticSearchQueryResult(final SearchRequestBuilder searchRequest, final Client client, final boolean containsAggregations) {
        this.searchRequest = searchRequest;
        this.containsAggregations = containsAggregations;
        ElasticSearchQueryResult.client = client;
    }
    
    public static SearchRequestBuilder modifyRequestForScrolling(final SearchRequestBuilder searchRequest) {
        return searchRequest.setSize(ElasticSearchQueryResult.scrollSize).setScroll(ElasticSearchQueryResult.scrollTimeout);
    }
    
    @Override
    public long size() {
        final SearchRequestBuilder scrollRequest = modifyRequestForScrolling(this.searchRequest);
        final SearchResponse searchResponse = (SearchResponse)scrollRequest.execute().actionGet();
        Utility.reportExecutionTime(ElasticSearchQueryResult.logger, searchResponse.getTookInMillis());
        return searchResponse.getHits().getTotalHits();
    }
    
    @Override
    public Iterator<HD> iterator() {
        if (this.containsAggregations) {
            this.searchRequest.setSize(0);
            return new HDQueryAggregationIterator(this.searchRequest);
        }
        if (ElasticSearchQueryResult.logger.isDebugEnabled()) {
            ElasticSearchQueryResult.logger.debug((Object)("Search request: " + this.searchRequest.toString()));
        }
        return new HDQueryIterator(this.searchRequest, ElasticSearchQueryResult.client, ElasticSearchQueryResult.scrollTimeout);
    }
    
    static {
        logger = Logger.getLogger((Class)ElasticSearchQueryResult.class);
        scrollSize = Integer.parseInt(System.getProperty("com.datasphere.config.elasticsearch.scrollsize", "100"));
        scrollTimeout = new TimeValue(60000L);
    }
}
