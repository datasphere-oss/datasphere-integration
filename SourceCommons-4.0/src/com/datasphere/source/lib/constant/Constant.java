package com.datasphere.source.lib.constant;

public class Constant
{
    public static final int EOF_DELAY_DEFAULT = 1000;
    public static final int CONNECTION_TIMEOUT = 1000;
    public static final int BLOCK_SIZE_DEFAULT = 64;
    public static final int COL_INDEX_TO_START_FROM = 0;
    public static int STRING_TYPE;
    public static final int PARTIAL_ROW = 0;
    public static final int FULL_ROW = 1;
    public static int MAX_COLUMN_OFFSET_DEFAULT;
    public static int HEADER_LINE_NO_DEFAULT;
    public static int MAX_COLUMN_COUNT;
    public static String REMOTE_ADDRESS_DEFAULT;
    public static int PORT_NO_DEFAULT;
    public static char STRING_LENGTH_SENTINEL;
    public static int BYTE_SIZE;
    public static int SHORT_SIZE;
    public static int INTEGER_SIZE;
    public static int DOUBLE_SIZE;
    public static int LONG_SIZE;
    public static final String TRIM_QUOTE = "trimquote";
    public static final String HEADER = "header";
    public static final String COMMENT_CHARACTER = "commentcharacter";
    public static final String LINE_NUMBER = "LineNumber";
    public static final String TRIM_WHITE_SPACE = "trimwhitespace";
    public static final String IGNORE_ROW_DELIMITER_IN_QUOTE = "IgnoreRowDelimiterInQuote";
    public static final String IGNORE_EMPTY_COLUM = "IgnoreEmptyColumn";
    public static final String COLUMN_DELIMIT_TILL = "columndelimittill";
    public static final String ROW_DELIMITER = "rowdelimiter";
    public static final String COLUMN_DELIMITER = "columndelimiter";
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final String DEFAULT_COLUMN_DELIMITER = ",";
    public static final String DELIMITER_SPLIT_CHAR = "separator";
    public static final String DEFAULT_DELIMITER_SPLIT_CHAR = ":";
    public static final String QUOTECHAR = "quotecharacter";
    public static final String QUOTE_SET = "quoteset";
    public static final String ESCAPE_SEQUENCE = "escapesequence";
    public static final String ESCAPE_CHARACTER = "escapecharacter";
    public static final String DEFAULT_QUOTE_SET;
    public static final String BLOCK_AS_COMPLETE_RECORD = "blockAsCompleteRecord";
    public static final String BREAK_ON_NO_RECORD = "breakonnorecord";
    public static final String PAIR_DELIMITER = "pairdelimiter";
    public static final String VALUE_DELIMITER = "valuedelimiter";
    public static final String BLOCK_SIZE = "blocksize";
    public static final String TIME_STAMP = "TimeStamp";
    public static final String RECORD_BEGIN = "RecordBegin";
    public static final String RECORD_END = "RecordEnd";
    public static final String KAFKA_RECORD_END = "KafkaRecordEnd";
    public static final String NO_COLUMN_DELIMITER = "nocolumndelimiter";
    public static final String HEADER_LINE_NO = "headerlineno";
    public static final String RECORD_BEGIN_WITH_TS = "RecordBeginWithTimeStamp";
    public static final String IGNORE_MULTIPLE_RECORD_BEGIN = "ignoremultiplerecordbegin";
    public static final String FILE_NAME = "FileName";
    public static final String FILE_OFFSET = "FileOffset";
    public static final String RECORD_STATUS = "RecordStatus";
    public static final String ORIGIN_TIME_STAMP = "OriginTimestamp";
    public static final String RECORD_OFFSET = "RecordOffset";
    public static final String EVENT_METADATA = "EventMetadata";
    public static final String CLIENT_IP_ADDRESS = "ClientIPAddress";
    public static final String CLIENT_PROTOCOL_VERSION = "ClientProtocolVersion";
    public static final String CLIENT_CONTENT_LENGTH = "ClientContentLength";
    public static final String CLIENT_URL = "ClientURL";
    public static final String REFERRER = "Referrer";
    public static final String META_TABLENAME = "TableName";
    public static final String META_OPERATIONNAME = "OperationName";
    public static final String META_TXNID = "TxnID";
    public static final String META_TXNSYSTEMNAME = "TxnSystemName";
    public static final String META_TXNUSERID = "TxnUserID";
    public static final String META_TIMESTAMP = "TimeStamp";
    public static final String META_ROWID = "ROWID";
    public static final String META_CATALOGOBJECTNAME = "CatalogObjectName";
    public static final String META_CATALOGOBJECTTYPE = "CatalogObjectType";
    public static final String META_TYPE_UUID = "typeUUID";
    
    
    
    public static String getCorrespondingClassForType(final fieldType type) {
        switch (type) {
            case BYTE: {
                return "java.lang.Byte";
            }
            case SHORT: {
                return "java.long.Short";
            }
            case INTEGER: {
                return "java.lang.Integer";
            }
            case LONG: {
                return "java.lang.Long";
            }
            case FLOAT: {
                return "java.lang.Float";
            }
            case DOUBLE: {
                return "java.lang.Double";
            }
            case STRING: {
                return "java.lang.String";
            }
            case TIMESTAMP:
            case TIME: {
                return "org.joda.time.DateTime";
            }
            case BINARY: {
                return "java.nio.ByteBuffer";
            }
            default: {
                throw new RuntimeException("Type " + type + " not a valid  Data type");
            }
        }
    }
    
    static {
        Constant.STRING_TYPE = 96;
        Constant.MAX_COLUMN_OFFSET_DEFAULT = 10240;
        Constant.HEADER_LINE_NO_DEFAULT = 1;
        Constant.MAX_COLUMN_COUNT = 10240;
        Constant.REMOTE_ADDRESS_DEFAULT = "127.0.0.1";
        Constant.PORT_NO_DEFAULT = 2012;
        Constant.STRING_LENGTH_SENTINEL = ';';
        Constant.BYTE_SIZE = 1;
        Constant.SHORT_SIZE = 2;
        Constant.INTEGER_SIZE = 4;
        Constant.DOUBLE_SIZE = 8;
        Constant.LONG_SIZE = 8;
        DEFAULT_QUOTE_SET = Character.toString('\"');
    }
    
    public enum status
    {
        NORMAL, 
        END_OF_COLUMN, 
        END_OF_COLUMN_QUOTED, 
        COLUMN_NAME_START, 
        COLUMN_NAME_END, 
        COLUMN_DATA_START, 
        COLUMN_IS_ROWID, 
        BEFORE_IMAGE_START, 
        END_OF_ROW, 
        ERROR, 
        OPEN_BRK, 
        CLOSE_BRK, 
        END_OF_COMMENT, 
        IN_COMMENT, 
        NOT_ACCEPTED, 
        MULTIPLE_STATUS;
    }
    
    public enum eventType
    {
        ON_OPEN, 
        ON_CLOSE;
    }
    
    public enum fieldType
    {
        BYTE, 
        SHORT, 
        INTEGER, 
        DOUBLE, 
        STRING, 
        TIME, 
        BINARY, 
        LONG, 
        FLOAT, 
        TIMESTAMP, 
        IP, 
        MACID, 
        BIGINTEGER;
    }
    
    public enum FormatOptions
    {
        Native, 
        Table, 
        Default;
    }
}
