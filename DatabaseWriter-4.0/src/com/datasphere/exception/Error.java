package com.datasphere.exception;

public enum Error
{
    TABLE_SPECIFIED_DONT_EXIST(2701, "Table specified don't exist."), 
    COLUMNS_DONT_EXIST_FOR_SPECIFIED_TABLES(2702, "Columns don't exist for tables specified."), 
    CONNECTIONURL_NOT_SPECIFIED(2703, "ConnectionURL is not specified.Please specify ConnectionURL parameter."), 
    INVALID_CONNECTIONURL_FORMAT(2704, "Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String."), 
    USERNAME_NOT_SPECIFIED(2705, "Username is not specified.Please specify Username parameter."), 
    INVALID_USERNAME_FORMAT(2706, "Invalid Username format.Value specified cannot be cast to java.lang.String."), 
    PASSWORD_NOT_SPECIFIED(2707, "Password is not specifiedPlease specify Password parameter."), 
    INVALID_PASSWORD_FORMAT(2708, "Invalid Password format.Value specified cannot be cast to java.lang.String."), 
    TABLES_NOT_SPECIFIED(2709, "Maps is not specifiedPlease specify Tables parameter."), 
    INVALID_TABLES_FORMAT(2710, "Invalid Maps format.Value specified cannot be cast to java.lang.String."), 
    FAILURE_CONNECTING_TO_DATABASE(2711, "Failure in connecting to Database."), 
    FAILURE_DISCONNECTING_FROM_DATABASE(2712, "Failure in disconnecting from Database."), 
    INVALID_TABLES_FORMAT_SOURCE_OR_TARGET_NOT_SPECIFIED(2713, "Invalid Tables format, only source or target table specified.Please specify both source and target tables eg: sourceTableName,targetTableName "), 
    ILLLEGAL_ARGUMENT_IN_TABLES_PROPERTY(2714, "Illegal argument in TABLES property found.Expected argument should contain at most 3 dot separated string."), 
    FAILURE_IN_FETCHING_TABLE_METADATA(2715, "Failure in fetching table metadata from Database."), 
    FAILURE_IN_FETCHING_COLUMN_METADATA(2716, "Failure in fetching column metadata from Database."), 
    FAILURE_IN_FETCHING_KEY_COLUMN_METADATA(2717, "Failure in fetching key column metadata from Database."), 
    MISSING_METADATA(2718, "We support Event having metadata section. Cannot process this Event."), 
    MISSING_METADATA_OPERATIONNAME(2719, "We support Event having metadata OperationName. Cannot process this Event."), 
    MISSING_DATAPRESENCEBITMAP(2720, "We support Event with OperationName INSERT/UPDATE/DELETE/SELECT having dataPresenceBitMap section. Cannot process this Event."), 
    MISSING_DATA(2721, "We support Event with OperationName INSERT/UPDATE/DELETE/SELECT having data section. Cannot process this Event."), 
    MISSING_METADATA_TABLENAME(2722, "We support Event having metadata TableName. Cannot process this Event."), 
    METADATA_TABLENAME_NULL(2723, "We support Event having valid name TableName. TableName is null. Cannot process this Event."), 
    METADATA_TARGETTABLENAME_NULL(2724, "Cannot process Event as there is no target table Mapped to source table: "), 
    NOT_HDEvent(2725, "We support event of HDEvent type only. Cannot process this event."), 
    QUERY_PROCESSING_FAILURE(2726, "Failure in Processing query."), 
    FAILURE_IN_EXECUTING_QUERY(2727, "Failure in Executing query."), 
    MISSING_TARGETTABLENAME(2728, "Cannot process this Event as there is no target table "), 
    SOURCE_AND_TARGET_COLUMNCOUNT_MISMATCH(2729, "Column count in Event(from source table:"), 
    MISSING_BEFOREPRESENCEBITMAP(2730, "We support Event with OperationName INSERT/UPDATE/DELETE/SELECT having beforePresenceBitMap section. Cannot process this Event."), 
    MISSING_KEYCOLUMN_IN_TARGET_TABLE(2731, "Target Table does not have key columns."), 
    MISSING_KEYCOLUMN_IN_HDEvent(2732, "Key column is not there in Event."), 
    FAILURE_IN_SHUTDOWN(2733, "Failure in closing."), 
    FAILURE_IN_TYPECASTING(2734, "Error converting datatype."), 
    COMMITPOLICY_INVALID(2735, "Specified CommitPolicy is invalid:"), 
    INVALID_COMMITPOLICY_FORMAT(2736, "Invalid CommitPolicy format.Value specified cannot be cast to java.lang.String."), 
    FAILURE_IN_SQL_QUERY(2737, "Failure in setting AutoCommit flag for connection to:"), 
    FAILURE_IN_COMMITSQL_QUERY(2738, "Failure in commiting records"), 
    BATCHPOLICY_INVALID(2739, "Specified BatchPolicy is invalid:"), 
    INVALID_BATCHPOLICY_FORMAT(2740, "Invalid BatchPolicy format.Value specified cannot be cast to java.lang.String."), 
    DEFINE_COMMITPOLICY_OR_BATCHPOLICY(2741, "Please specify CommitPolicy or BatchPolicy, You cannot use CommitPolicy and BatchPolicy together."), 
    FAILURE_IN_BEGIN(2742, "Failure in setting txn begin"), 
    FAILURE_IN_COMMIT(2743, "Failure in commit"), 
    FAILURE_IN_ROLLBACK(2744, "Failure in rollback"), 
    MISSING_BEFORE_IMG(2745, "Before image is NULL, Please check 'SendBeforeImage' property of source"), 
    CONNECTION_CLOSED(2746, "JDBC connection cloded"), 
    TARGET_TABLE_DOESNOT_EXISTS(2747, "Target table does not exist"), 
    INCORRECT_CHECKPOINT_TABLE_STRCUTRE(2748, "Incorrect checkpoint table structure"), 
    INCORRECT_TABLE_MAP(2749, "Incorrect table map specified in Tables property"), 
    NO_OP_UPDATE(2750, "NO-OP Update"), 
    INCONSISTENT_TABLE_STRUCTURE(2751, "Inconsistent source & target table strcture");
    
    private int type;
    private String text;
    
    private Error(final int type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
    
    public int getType() {
        return this.type;
    }
}
