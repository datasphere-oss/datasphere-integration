package com.datasphere.mongodbreader.utils;

public enum AuthType
{
    NoAuth, 
    Default, 
    SCRAMSHA1, 
    MONGODBCR, 
    GSSAPI, 
    PLAIN, 
    MONGODBX509;
}
