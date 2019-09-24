package com.datasphere.common.errors;

public enum Error implements IError
{
    GENERIC_EXCEPTION(100, "Unexpected Exception"), 
    NOT_IMPLEMENTED(101, "Method Not Implemented"), 
    GENERIC_IO_EXCEPTION(102, "Unexpected I/0 Exception"), 
    GENERIC_INTERRUPT_EXCEPTION(103, "Unexpected Interrupt Received"), 
    CONNECTION_RETRY_EXCEEDED(104, "Number of connection retry attempts exceeded allowable retry attempts"), 
    HOST_CONNECTION_DROPPED(105, "Connection with remote host is broken"), 
    INVALID_DIRECTORY(301, "Directory path is Invalid "), 
    BROKEN_LINK(309, "Path specified contains a broken link"), 
    INVALID_INPUTDATA(301, "Input data Invalid"), 
    FILE_NOT_FOUND(305, "File is not found in the directory specified"), 
    FILE_TRUNCATED(306, "File has been truncated"), 
    INVALID_CREATION_TIME(307, "File with invalid creation time exists"), 
    END_OF_DATASOURCE(308, "Reached end of source stream"), 
    INVALID_PROPERTY(301, "Property is Invalid"), 
    UNSUPPORTED_CHARSET_NAME(302, "No support for the named charset is available in this instance of the Java virtual machine"), 
    UNSUPORTED_DATATYPE(301, "Field type is not supported"), 
    INVALID_JSON_NODE(301, "Invalid node is passed"), 
    METAFILE_NOTFOUND(301, "Please set the meta data file path"), 
    INVALID_METAFORMAT(301, "Invalid json file"), 
    INVALID_ERROR_LOGFORMAT(301, "Invalid Log Format"), 
    INVALID_LOGFORMAT(301, "Log format is Invalid"), 
    FAILED_SESSION_INITIALIZATION(1001, "Session Initialization is failed"), 
    ERROR_READ_CONFIGURATION_FILE(1002, "Error in reading configuration file"), 
    MISSING_SCHEMANAME(1003, "Schema Name is not specified"), 
    MISSING_USERNAME(1004, "Database User Name is not specified"), 
    MISSING_PASSWORD(1005, "Database password is not specified"), 
    MISSING_CONNECTSTRING(1006, "Connect String URL is not specified"), 
    MISSING_TABLELIST(1007, "Table List is not specified"), 
    INVALID_USERNAME(1008, "Invalid Database User Name format.Value specified cannot be cast to java.lang.String"), 
    INVALID_PASSWORD(1009, "Invalid Database password format. Value specified cannot be cast to java.lang.String"), 
    INVALID_CONNECTSTRING(1010, "Invalid Connect String URL format. Value specified cannot be cast to java.lang.String"), 
    INVALID_TABLELIST(1011, "Invalid Table List format. Value specified cannot be cast to java.lang.String"), 
    INVALID_TABLENAME(1012, "Specified table name is incorrect."), 
    BUFFER_LIMIT_EXCEED_ERROR(2006, "Incoming Buffer size is more than the maximum size"), 
    INVALID_PROTOCOL_BUFFER(2007, "Invalid Protocol Buffer Format"), 
    INVALID_TABLE_FORMAT(2008, "Table cannot contain both interested and excluded columns"), 
    INVALID_DATA_FIELD(2009, "Data field is empty"), 
    INVALID_INDEX(2010, "Index is out of range"), 
    FAILED_SOCKET_INITIALIZATION(2011, "Failed in Socket initialization"), 
    INVALID_AUDITTRAILLIST(1011, "Invalid Audit Trail List format. Value specified cannot be cast to java.lang.String"), 
    EXCEPTION_RECIEVED(1012, "CDCException Recieved"), 
    INVALID_SESSIONNAME(1013, "Invalid session name of CDCProcess. "), 
    MISSING_DATABASENAME(2012, "Database Name is not specified"), 
    INVALID_DATABASENAME(2013, "Invalid Database Name format.Value specified cannot be cast to java.lang.String"), 
    INVALID_IP_ADDRESS(3001, "IP address of the host could not be determined"), 
    LISTENER_NOT_CONNECTABLE(3002, "Unable to connect to CDCProcess"), 
    LISTENER_COMMUNICATION_ERROR(3003, "Unable to reach CDCProcess"), 
    AGENT_COMMUNICATION_ERROR(3004, "Unable to reach Agent"), 
    INVALID_LISTENER_DETAILS(3005, "CDCProcess details are not valid"), 
    ERROR_IN_AGENT_RESPONSE(3006, "Agent has reported an error"), 
    ERROR_IN_PROCESSING_CDCRECORDS(3007, "Error while processing CDCRecords"), 
    LISTENER_NOT_INITIALIZED(3008, "CDCProcess isn't initialized yet, not ready for serving CDCReader."), 
    FAILURE_IN_ENDING_SOCKET_COMMUNICATION(3009, "Failed to close the client socket"), 
    CDCPROCESS_LAUNCH_FAILURE(3009, "Agent failed to launch a CDCProcess. CDCReader shutting down !!"), 
    INVALID_METARECORD_ID(4001, "Could not locate metadata record id for given data record :"), 
    INVALID_HDFS_CONFIGURATION(5001, "Problem configuring HDFS, please check HDFS configuration parameters"), 
    INVALID_KEYSTORE_LOCATION(5002, "Keystore location is empty please specify a valid location"), 
    INVALID_LDAP_CONFIGURATION(6001, "LDAP configuration is invalid"), 
    NOT_ERROR(0, "Not an error");
    
    public int type;
    public String text;
    
    private Error(final int type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
}
