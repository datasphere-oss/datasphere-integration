package com.datasphere.hdstore.elasticsearch;

import org.apache.log4j.*;
import org.elasticsearch.common.unit.*;
import org.elasticsearch.client.*;
import org.elasticsearch.search.*;
import org.elasticsearch.action.search.*;
import com.fasterxml.jackson.databind.node.*;
import com.datasphere.hdstore.*;
import java.util.*;

class HDQueryIterator implements Iterator<HD>
{
    private static final Logger logger;
    private final TimeValue scrollTimeout;
    private final Client client;
    private SearchResponse searchResponse;
    private Iterator<SearchHit> results;
    
    HDQueryIterator(final SearchRequestBuilder searchRequest, final Client client, final TimeValue scrollTimeout) {
        final SearchRequestBuilder scrollRequest = ElasticSearchQueryResult.modifyRequestForScrolling(searchRequest);
        this.searchResponse = (SearchResponse)scrollRequest.execute().actionGet();
        this.client = client;
        this.scrollTimeout = scrollTimeout;
        this.results = (Iterator<SearchHit>)this.searchResponse.getHits().iterator();
    }
    
    @Override
    public boolean hasNext() {
        boolean result = this.results.hasNext();
        if (!result) {
            final String scrollId = this.searchResponse.getScrollId();
            this.searchResponse = (SearchResponse)this.client.prepareSearchScroll(scrollId).setScroll(this.scrollTimeout).execute().actionGet();
            this.results = (Iterator<SearchHit>)this.searchResponse.getHits().iterator();
            result = this.results.hasNext();
        }
        return result;
    }
    
    @Override
    public HD next() {
        HD hd = null;
        final SearchHit result = this.results.next();
        if (result != null) {
            hd = new HD();
            final Map<String, Object> source = (Map<String, Object>)result.getSource();
            final ObjectNode node = (ObjectNode)Utility.objectMapper.valueToTree((Object)source);
            hd.setAll(node);
        }
        if (HDQueryIterator.logger.isDebugEnabled()) {
            HDQueryIterator.logger.debug((Object)String.format("Returning HD '%s'", hd));
        }
        return hd;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    static {
        logger = Logger.getLogger((Class)HDQueryIterator.class);
    }
}
