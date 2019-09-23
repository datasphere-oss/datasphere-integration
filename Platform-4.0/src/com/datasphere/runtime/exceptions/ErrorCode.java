package com.datasphere.runtime.exceptions;

public class ErrorCode
{
    public enum Error
    {
        CLUSTERNAME(101, "Cluster name may not be present or is set to null."), 
        CLUSTERPASSWORD(102, "Cluster password may not be present or is set to null."), 
        DATABASELOCATION(103, "Cannot connect to DB server at specified location."), 
        DATABASENAME(104, "Cannot find DB at specified location."), 
        DATABASEUNAME(105, "Cannot conenct to DB with the user name that was passed in."), 
        DATABASEPASS(106, "DB password for the user name passed in did not match."), 
        INTERFACES(107, "No Interfaces available from configuration file."), 
        INVALIDPASS(108, "Invalid Password."), 
        FILENOTFOUND(109, "File not found."), 
        SERVERCANNOTSTART(110, "Server cannot start."), 
        NOLICENSEKEYSET(111, "License Key is not set"), 
        NOPRODUCTKEYSET(112, "Product Key is not set"), 
        NOCOMPANYNAMESET(113, "No company name is set"), 
        GRANTROLEFAILURE(114, "Unable to grant role to user"), 
        NOERROR(0, "No error");
        
        private final int code;
        private final String description;
        
        private Error(final int code, final String description) {
            this.code = code;
            this.description = description;
        }
        
        public String getDescription() {
            return this.description;
        }
        
        public int getCode() {
            return this.code;
        }
        
        @Override
        public String toString() {
            return "ERROR " + this.code + ": " + this.description;
        }
    }
}
