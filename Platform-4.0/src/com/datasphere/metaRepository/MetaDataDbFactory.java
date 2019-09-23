package com.datasphere.metaRepository;

import org.apache.log4j.*;

public class MetaDataDbFactory
{
    public static final String DERBY = "derby";
    public static final String ORACLE = "oracle";
    public static final String POSTGRES = "postgres";
    private static Logger logger;
    private static MetaDataDbProvider ourMetaDataDbProvider;
    
    public static MetaDataDbProvider getOurMetaDataDb(final String dataBaseName) {
        switch (dataBaseName) {
            case "derby": {
                if (MetaDataDbFactory.ourMetaDataDbProvider == null) {
                    MetaDataDbFactory.ourMetaDataDbProvider = new DerbyMD();
                    break;
                }
                break;
            }
            case "oracle": {
                if (MetaDataDbFactory.ourMetaDataDbProvider == null) {
                    MetaDataDbFactory.ourMetaDataDbProvider = new OracleMD();
                    break;
                }
                break;
            }
            case "postgres": {
                if (MetaDataDbFactory.ourMetaDataDbProvider == null) {
                    MetaDataDbFactory.ourMetaDataDbProvider = new PostgresMD();
                    break;
                }
                break;
            }
        }
        return MetaDataDbFactory.ourMetaDataDbProvider;
    }
    
    static {
        MetaDataDbFactory.logger = Logger.getLogger((Class)MetaDataDbFactory.class);
        MetaDataDbFactory.ourMetaDataDbProvider = null;
    }
}
