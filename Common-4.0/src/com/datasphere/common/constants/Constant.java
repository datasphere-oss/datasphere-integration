package com.datasphere.common.constants;

public class Constant
{
    public static String ORACLE_TYPE;
    public static String MYSQL_TYPE;
    public static String MSSQL_TYPE;
    public static String POSTGRESS_TYPE;
    public static String HIVE_TYPE;
    public static String EDB_TYPE;
    public static String OPERATION;
    public static String OPERATION_TYPE;
    public static String DDL_OPERATION;
    public static String OPERRATION_SUB_NAME;
    public static String CATALOG_OBJ_TYPE;
    public static String INSERT_OPERATION;
    public static String SELECT_OPERATION;
    public static String INITIAL_LOAD_COMPLETED_OPERATION;
    public static String UPDATE_OPERATION;
    public static String DELETE_OPERATION;
    public static String CREATE_OPERATION;
    public static String ALTER_OPERATION;
    public static String DROP_OPERATION;
    public static String CREATE_TABLE_AS;
    public static String TRUNCATE_OPERATION;
    public static String RENAME_OPERATION;
    public static String ANALYZE_OPERATION;
    public static String GRANT_OPERATION;
    public static String REVOKE_OPERATION;
    public static String PURGE_OPERATION;
    public static String TABLE_OBJ_TYPE;
    public static String VIEW_OBJ_TYPE;
    public static String FUNCTION_OBJ_TYPE;
    public static String PROCEDURE_OBJ_TYPE;
    public static String PACKAGE_OBJ_TYPE;
    public static String UKNOWN_OBJ_TYPE;
    public static String INDEX_OBJ_TYPE;
    public static String SEQUENCE_OBJ_TYPE;
    public static String CLUSTER_OBJ_TYPE;
    public static String UNKONWN_OBJECT_TYPE;
    public static String TABLE_META_DATA;
    public static String CATALOG_OBJECT_TYPE;
    public static String CATALOG_NAME;
    public static String SCHEMA_NAME;
    public static String OBJECT_NAME;
    public static String TABLE_NAME;
    public static String CONNECTION_URL;
    public static String TARGET_UUID;
    public static String TARGET_IDENTIFIER;
    public static String CHECK_POINT_TABLE;
    public static String USER;
    public static String PASSWORD;
    public static String ENCRYPTED;
    public static String TABLES_PROPETY;
    public static String DBCONNECTION;
    public static String ENABLE_IDENTITY_INSERT;
    public static String RECOVERY_ENABLED;
    
    static {
        Constant.ORACLE_TYPE = "Oracle";
        Constant.MYSQL_TYPE = "MySQL";
        Constant.MSSQL_TYPE = "sqlserver";
        Constant.POSTGRESS_TYPE = "postgresql";
        Constant.HIVE_TYPE = "hive2";
        Constant.EDB_TYPE = "edb";
        Constant.OPERATION = "OperationName";
        Constant.OPERATION_TYPE = "OperationType";
        Constant.DDL_OPERATION = "DDL";
        Constant.OPERRATION_SUB_NAME = "OperationSubName";
        Constant.CATALOG_OBJ_TYPE = "CatalogObjectType";
        Constant.INSERT_OPERATION = "Insert";
        Constant.SELECT_OPERATION = "Select";
        Constant.INITIAL_LOAD_COMPLETED_OPERATION = "INITIAL_LOAD_COMPLETED";
        Constant.UPDATE_OPERATION = "Update";
        Constant.DELETE_OPERATION = "Delete";
        Constant.CREATE_OPERATION = "CREATE";
        Constant.ALTER_OPERATION = "ALTER";
        Constant.DROP_OPERATION = "DROP";
        Constant.CREATE_TABLE_AS = "CTAS";
        Constant.TRUNCATE_OPERATION = "TRUNCATE";
        Constant.RENAME_OPERATION = "RENAME";
        Constant.ANALYZE_OPERATION = "ANALYZE";
        Constant.GRANT_OPERATION = "GRANT";
        Constant.REVOKE_OPERATION = "REVOKE";
        Constant.PURGE_OPERATION = "PURGE";
        Constant.TABLE_OBJ_TYPE = "TABLE";
        Constant.VIEW_OBJ_TYPE = "VIEW";
        Constant.FUNCTION_OBJ_TYPE = "FUNCTION";
        Constant.PROCEDURE_OBJ_TYPE = "PROCEDURE";
        Constant.PACKAGE_OBJ_TYPE = "PACKAGE";
        Constant.UKNOWN_OBJ_TYPE = "ObjectTypeUnknown";
        Constant.INDEX_OBJ_TYPE = "INDEX";
        Constant.SEQUENCE_OBJ_TYPE = "SEQUENCE";
        Constant.CLUSTER_OBJ_TYPE = "CLUSTER";
        Constant.UNKONWN_OBJECT_TYPE = "UNKNOWN";
        Constant.TABLE_META_DATA = "TableMetadata";
        Constant.CATALOG_OBJECT_TYPE = "CatalogObjectType";
        Constant.CATALOG_NAME = "CatalogName";
        Constant.SCHEMA_NAME = "SchemaName";
        Constant.OBJECT_NAME = "ObjectName";
        Constant.TABLE_NAME = "TableName";
        Constant.CONNECTION_URL = "ConnectionURL";
        Constant.TARGET_UUID = "TargetUUID";
        Constant.TARGET_IDENTIFIER = "TargetIdentifier";
        Constant.CHECK_POINT_TABLE = "CheckPointTable";
        Constant.USER = "UserName";
        Constant.PASSWORD = "Password";
        Constant.ENCRYPTED = "encrypted";
        Constant.TABLES_PROPETY = "Tables";
        Constant.DBCONNECTION = "DBConnection";
        Constant.ENABLE_IDENTITY_INSERT = "EnableIdentityInsert";
        Constant.RECOVERY_ENABLED = "RecoveryEnabled";
    }
    
    public enum recordstatus
    {
        NO_RECORD, 
        VALID_RECORD, 
        INVALID_RECORD, 
        ERROR_RECORD, 
        END_OF_DATASOURCE;
    }
}
