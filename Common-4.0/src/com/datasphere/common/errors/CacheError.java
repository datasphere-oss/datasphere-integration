package com.datasphere.common.errors;

public enum CacheError implements IError
{
    NO_PRIMARY_KEY_SELECTED("DA-CACHE-1001", "No primary key selected."), 
    UPSERT_FAIL("DA-CACHE-1002", "Exception receiving records by cache"), 
    CACHE_RECORD_EXTRACTOR_CANNOT_INSTANTIATE("DA-CACHE-1003", "Cache record extractor cannot be initiated"), 
    CACHE_RECORD_EXTRACTOR_CANNOT_ACCESS("DA-CACHE-1004", " Cache record extractor constructor cannot be accessed."), 
    CACHE_POPULATE_ERROR("DA-CACHE-1005", "Problem in cache loading"), 
    CACHE_TYPE_NOT_FOUND("DA-CACHE-1006", "Cache type not found."), 
    CACHE_READER_FAILURE("DA-CACHE-1007", "Cache adapter failed to receive the record."), 
    EVENTTABLE_QUERY_FAILED("DA-CACHE-1008", "Query to elasticsearch failed."), 
    EVENTTABLE_DOCUMENT_READ_FAIL("DA-CACHE-1009", "Document retrieved from Elasticsearch cannot be parsed."), 
    CACHE_SECURITY_VIOLATION("DA-CACHE-1010", "Security Violation due to access pattern, or a java standard"), 
    INVALID_RECORD_LENGTH("DA-CACHE-1011", "The size of this record is invalid"), 
    FIELD_CONVERSION_FAILURE("DA-CACHE-1012", "Field conversion failed"), 
    NULL_KEY("DA-CACHE-1013", "Null key"), 
    CACHE_INCOMPATIBLE_ADAPTER("DA-CACHE-1014", "This Adapter isn't supported to be used with the cache"), 
    MDLOCALCACHE_INITIALIZATION_ERROR("DA-MDR-1001", "Error while initialzing MDLocalCache");
    
    public String type;
    public String text;
    
    private CacheError(final String type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
}
