package com.datasphere.proc.exception;

import org.apache.log4j.*;
import java.util.*;
import org.apache.kudu.client.*;

public class IgnoreException
{
    private static Logger logger;
    
    public boolean checkExceptionList(final List<String> exceptionList, final RowError rowError) {
        boolean status = false;
        for (int i = 0; i < exceptionList.size(); ++i) {
            if (!status) {
                final String s = exceptionList.get(i);
                switch (s) {
                    case "ABORTED": {
                        if (rowError.getErrorStatus().isAborted()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "ALREADY_PRESENT": {
                        if (rowError.getErrorStatus().isAlreadyPresent()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "CONFIGURATION_ERROR": {
                        if (rowError.getErrorStatus().isConfigurationError()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "CORRUPTION": {
                        if (rowError.getErrorStatus().isCorruption()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "END_OF_FILE": {
                        if (rowError.getErrorStatus().isEndOfFile()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "ILLEGAL_STATE": {
                        if (rowError.getErrorStatus().isIllegalState()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "INVALID_ARGUMENT": {
                        if (rowError.getErrorStatus().isInvalidArgument()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "INCOMPLETE": {
                        if (rowError.getErrorStatus().isIncomplete()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "IO_ERROR": {
                        if (rowError.getErrorStatus().isIOError()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "NETWORK_ERROR": {
                        if (rowError.getErrorStatus().isNetworkError()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "NOT_AUTHORIZED": {
                        if (rowError.getErrorStatus().isNotAuthorized()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "NOT_FOUND": {
                        if (rowError.getErrorStatus().isNotFound()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "NOT_SUPPORTED": {
                        if (rowError.getErrorStatus().isNotSupported()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "REMOTE_ERROR": {
                        if (rowError.getErrorStatus().isRemoteError()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "RUNTIME_ERROR": {
                        if (rowError.getErrorStatus().isRuntimeError()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "SERVICE_UNAVAILABLE": {
                        if (rowError.getErrorStatus().isServiceUnavailable()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "TIMED_OUT": {
                        if (rowError.getErrorStatus().isTimedOut()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                    case "UNINITIALIZED": {
                        if (rowError.getErrorStatus().isUninitialized()) {
                            status = true;
                            IgnoreException.logger.warn((Object)("ignoring this error" + rowError.toString()));
                            break;
                        }
                        break;
                    }
                }
            }
        }
        return status;
    }
    
    static {
        IgnoreException.logger = Logger.getLogger((Class)IgnoreException.class);
    }
}
