package com.datasphere.hdstore.elasticsearch;

import org.apache.log4j.*;
import java.util.*;
import org.elasticsearch.action.search.*;
import com.datasphere.hdstore.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.metrics.valuecount.*;

class HDQueryAggregationIterator implements Iterator<HD>
{
    private static final Class<HDQueryAggregationIterator> thisClass;
    private static final Logger logger;
    private final Collection<HD> hds;
    private final Iterator<HD> results;
    
    HDQueryAggregationIterator(final SearchRequestBuilder searchRequest) {
        this.hds = new ArrayList<HD>(0);
        final SearchResponse searchResponse = (SearchResponse)searchRequest.execute().actionGet();
        Utility.reportExecutionTime(HDQueryAggregationIterator.logger, searchResponse.getTookInMillis());
        this.collectHDs(new HD(), searchResponse.getAggregations());
        this.results = this.hds.iterator();
    }
    
    @Override
    public boolean hasNext() {
        return this.results.hasNext();
    }
    
    @Override
    public HD next() {
        final HD hd = this.results.next();
        if (HDQueryAggregationIterator.logger.isDebugEnabled()) {
            HDQueryAggregationIterator.logger.debug((Object)String.format("Returning HD '%s'", hd));
        }
        return hd;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    private void collectHDs(final HD hd, final Aggregations aggregations) {
        for (final Aggregation aggregation : aggregations) {
            if (aggregation instanceof Terms) {
                this.collectionHDGroups(hd, (Terms)aggregation);
                return;
            }
            if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                final String aggregationName = aggregation.getName();
                final double aggregationValue = ((InternalNumericMetricsAggregation.SingleValue)aggregation).value();
                hd.put(aggregationName, (aggregationValue <= Double.NEGATIVE_INFINITY) ? null : aggregationValue);
            }
            if (!(aggregation instanceof InternalValueCount)) {
                continue;
            }
            final String aggregationName = aggregation.getName();
            final long aggregationValue2 = ((ValueCount)aggregation).getValue();
            hd.put(aggregationName, aggregationValue2);
        }
        this.hds.add(hd);
    }
    
    private void collectionHDGroups(final HD baseHD, final Terms terms) {
        final String groupByName = terms.getName().replace(":groupby", "");
        for (final Terms.Bucket bucket : terms.getBuckets()) {
            final HD hd = new HD(baseHD);
            final String termValue = (String)bucket.getKey();
            hd.put(groupByName, termValue);
            this.collectHDs(hd, bucket.getAggregations());
        }
    }
    
    static {
        thisClass = HDQueryAggregationIterator.class;
        logger = Logger.getLogger((Class)HDQueryAggregationIterator.thisClass);
    }
}
