package com.datasphere.tungsten;

public interface TungstenConstants
{
    public static final String SERVER_SOCKET_URL = "ws://localhost:9080/rmiws/";
    public static final String JSON_DOC = "{\"class\":\"com.datasphere.runtime.QueryValidator\", \"method\":\"storeCommand\",\"params\":[\"#sessionid#\", \"#userid#\", \"#command#\"],\"callbackIndex\":2}";
}
