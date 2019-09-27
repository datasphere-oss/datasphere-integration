package com.datasphere.Fetchers;

public class FetcherFactory
{
    public static String METADATA_FETCHER_MARK;
    public static String USERDATA_FETCHER_MARK;
    public static char ENVIRONMENT_FETCHER_MARK;
    public static char STATIC_VALUE_MARK;
    
    public static Fetcher createFetcher(final String key) {
        final FetcherType type = getFetcherType(key);
        if (type == FetcherType.META) {
            return new MetaDataFetcher(key);
        }
        if (type == FetcherType.USERDATA) {
            return new UserDataFetcher(key);
        }
        final String tmpKey = key.trim().substring(1, key.length());
        if (type == FetcherType.ENV) {
            return new EnvFetcher(tmpKey);
        }
        if (type == FetcherType.STATIC) {
            return new StaticValueFetcher(tmpKey.substring(0, tmpKey.length() - 1));
        }
        return null;
    }
    
    public static FetcherType getFetcherType(String key) {
        key = key.trim().toUpperCase();
        if (key.charAt(0) == FetcherFactory.ENVIRONMENT_FETCHER_MARK) {
            return FetcherType.ENV;
        }
        if (key.charAt(0) == FetcherFactory.STATIC_VALUE_MARK) {
            return FetcherType.STATIC;
        }
        if (!key.startsWith(FetcherFactory.METADATA_FETCHER_MARK) && !key.startsWith(FetcherFactory.USERDATA_FETCHER_MARK)) {
            return FetcherType.UNKNOWN;
        }
        String[] parts = null;
        String remainder = null;
        parts = key.split(FetcherFactory.METADATA_FETCHER_MARK);
        if (parts != null && parts.length > 1) {
            remainder = parts[1].trim();
            if (remainder.startsWith("(") && remainder.endsWith(")")) {
                return FetcherType.META;
            }
            return FetcherType.UNKNOWN;
        }
        else {
            parts = key.split(FetcherFactory.USERDATA_FETCHER_MARK);
            if (parts == null || parts.length <= 1) {
                return FetcherType.UNKNOWN;
            }
            remainder = parts[1].trim();
            if (remainder.startsWith("(") && remainder.endsWith(")")) {
                return FetcherType.USERDATA;
            }
            return FetcherType.UNKNOWN;
        }
    }
    
    public static Fetcher createFetcher(final int index) {
        return new EventFetcher(index);
    }
    
    static {
        FetcherFactory.METADATA_FETCHER_MARK = "@METADATA";
        FetcherFactory.USERDATA_FETCHER_MARK = "@USERDATA";
        FetcherFactory.ENVIRONMENT_FETCHER_MARK = '$';
        FetcherFactory.STATIC_VALUE_MARK = '\"';
    }
    
    public enum FetcherType
    {
        META, 
        USERDATA, 
        ENV, 
        STATIC, 
        UNKNOWN;
    }
}
