package com.datasphere.runtime.deployment;

public class Constant
{
    public static String REMOTE_CALL_MSG;
    public static String DEPLOY_START_MSG;
    public static String ALREAD_LOADED_MSG;
    public static String ALREAD_UNLOADED_MSG;
    public static String OPERATION_NOT_SUPPORTED;
    public static String LOAD_FAILED_WITH_NO_IMPLICIT_APP;
    
    static {
        Constant.REMOTE_CALL_MSG = "%s call on object %s received with %d parameters from client with session ID %s";
        Constant.DEPLOY_START_MSG = "\nDEPLOY APPLICATION %s; \nSTART APPLICATION %s;";
        Constant.ALREAD_LOADED_MSG = "%s %s failed, it is already loaded.";
        Constant.ALREAD_UNLOADED_MSG = "%s %s failed, it is already unloaded.";
        Constant.OPERATION_NOT_SUPPORTED = "%s operation not supported.";
        Constant.LOAD_FAILED_WITH_NO_IMPLICIT_APP = "There was a problem in creating implicit application for object: %s";
    }
}
