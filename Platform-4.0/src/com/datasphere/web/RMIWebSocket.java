package com.datasphere.web;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.ConnectionClosedException;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

import com.datasphere.distribution.HQueue;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.CompilationException;
import com.datasphere.exception.SecurityException;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.IMap;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class RMIWebSocket implements WebSocket.OnTextMessage
{
    private static Logger logger;
    private static long TEN_MINUTES;
    private static long ONE_MINUTE;
    static IMap<UUID, List<AuthToken>> serverAuthTokens;
    static ConcurrentHashMap<AuthToken, Long> keepAlive;
    AuthToken userAuthToken;
    WebSocket.Connection connection;
    long count;
    private ObjectMapper mapper;
    boolean closed;
    private static Map<String, Map<String, Method>> classMap;
    private static Map<String, Object> objectMap;
    private final Map<String, HQueue.Listener> listenerMap;
    public static final String CALLBACKINDEX_CRUD = "metadata_updated";
    public static final String CALLBACKINDEX_STATUSCHANGE = "status_change";
    public static final String CLASS = "class";
    public static final String METHOD = "method";
    public static final String PARAMS = "params";
    public static final String CALLBACKINDEX = "callbackIndex";
    public static final String SUCCESS = "success";
    Map<UUID, Map<String, UUID>> alertSubscriptionIds;
    
    public static void register(final Object anyObject) throws RMIWebSocketException {
        final Class<?> anyClass = anyObject.getClass();
        final String className = anyClass.getCanonicalName();
        RMIWebSocket.objectMap.put(className, anyObject);
        final Map<String, Method> methodMap = new HashMap<String, Method>();
        RMIWebSocket.classMap.put(className, methodMap);
        if (RMIWebSocket.logger.isInfoEnabled()) {
            RMIWebSocket.logger.info((Object)("Registering class " + className));
        }
        final Method[] declaredMethods;
        final Method[] allMethods = declaredMethods = anyClass.getDeclaredMethods();
        for (final Method method : declaredMethods) {
            if (Modifier.isPublic(method.getModifiers())) {
                final String methodName = method.getName();
                final Class<?>[] types = method.getParameterTypes();
                final String methodKey = methodName + ":" + types.length;
                if (methodMap.containsKey(methodKey)) {
                    throw new RMIWebSocketException("Duplicate method with name " + methodName + " and parameter count " + method.getParameterTypes().length + " defined in " + className);
                }
                if (RMIWebSocket.logger.isDebugEnabled()) {
                    RMIWebSocket.logger.debug((Object)("Added method " + methodKey));
                }
                methodMap.put(methodKey, method);
            }
        }
    }
    
    public RMIWebSocket() {
        this.userAuthToken = null;
        this.count = 0L;
        this.closed = true;
        this.listenerMap = new HashMap<String, HQueue.Listener>();
        this.alertSubscriptionIds = new HashMap<UUID, Map<String, UUID>>();
        (this.mapper = ObjectMapperFactory.newInstance()).setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }
    
    public void onClose(final int arg0, final String arg1) {
        this.closed = true;
        synchronized (this.listenerMap) {
            for (final Map.Entry<String, HQueue.Listener> entry : this.listenerMap.entrySet()) {
                final QueueSender s = (QueueSender)entry.getValue();
                s.queue.unsubscribe(s);
                if (s.queueName.startsWith("consoleQueue")) {
                    final String clientUUID = s.queueName.replace("consoleQueue", "");
                    try {
                        Server.server.adhocCleanup(clientUUID, null);
                    }
                    catch (Exception e) {
                        RMIWebSocket.logger.error((Object)"Adhoc clean up failed", (Throwable)e);
                    }
                }
            }
            this.listenerMap.clear();
        }
        WebServer.removeMember(this);
    }
    
    public void onOpen(final WebSocket.Connection c) {
        this.connection = c;
        this.closed = false;
        this.connection.setMaxTextMessageSize(1048576);
        WebServer.addMember(this);
    }
    
    private String getJsonNodeValue(final JsonNode rootNode, final String valueName, final String jsonMsg) throws RMIWebSocketException {
        final JsonNode valueNode = rootNode.get(valueName);
        if (valueNode == null) {
            throw new RMIWebSocketException("The '" + valueName + "' element must be set in JSON provided - " + jsonMsg);
        }
        return valueNode.asText();
    }
    
    private boolean handleSpecialMessages(final String className, final String methodName, final String callbackIndex, final List<String> paramStrList) {
        try {
            if ("list".equalsIgnoreCase(methodName)) {
                this.list(callbackIndex);
                return true;
            }
            if ("ping".equalsIgnoreCase(methodName)) {
                this.ping(callbackIndex);
                return true;
            }
            if ("subscribe".equalsIgnoreCase(methodName)) {
                if (paramStrList.size() != 2) {
                    throw new RMIWebSocketException("Subscribe needs 2 parameters - queueName and authToken");
                }
                final String queueName = (String)this.mapper.readValue((String)paramStrList.get(0), (Class)String.class);
                final AuthToken token = (AuthToken)this.mapper.readValue((String)paramStrList.get(1), (Class)AuthToken.class);
                this.subscribeToQueue(callbackIndex, queueName, token);
                return true;
            }
            else {
                if (!"unsubscribe".equalsIgnoreCase(methodName)) {
                    throw new RMIWebSocketException("The method '" + methodName + "' for class '" + className + "' does not exist.");
                }
                if (paramStrList.size() != 3) {
                    throw new RMIWebSocketException("Unsubscribe needs 3 parameters - queueName, id and authToken");
                }
                final String queueName = (String)this.mapper.readValue((String)paramStrList.get(0), (Class)String.class);
                final String id = (String)this.mapper.readValue((String)paramStrList.get(1), (Class)String.class);
                final AuthToken token2 = (AuthToken)this.mapper.readValue((String)paramStrList.get(2), (Class)AuthToken.class);
                this.unsubscribeFromQueue(callbackIndex, queueName, id, token2);
                return true;
            }
        }
        catch (Exception e) {
            this.handleMessageException(callbackIndex, e);
            return false;
        }
    }
    
    private void handleMessageException(final String callbackIndex, final Exception e) {
        this.handleMessageException(callbackIndex, e, null);
    }
    
    private void handleMessageException(final String callbackIndex, final Exception e, String JSONMessage) {
        if (JSONMessage == null) {
            JSONMessage = "";
        }
        else {
            JSONMessage = " : " + JSONMessage;
        }
        try {
            this.call(false, callbackIndex, e.getMessage(), e.getClass().getCanonicalName());
            if (e instanceof RMIWebSocketException) {
                final Throwable eInner = e.getCause();
                if (eInner instanceof InvocationTargetException) {
                    final Throwable eBase = ((InvocationTargetException)eInner).getTargetException();
                    if (!(eBase instanceof CompilationException)) {
                        RMIWebSocket.logger.error((Object)("Problem executing call: " + e.getMessage() + JSONMessage), (Throwable)e);
                    }
                }
            }
        }
        catch (RMIWebSocketException e3) {
            RMIWebSocket.logger.error((Object)("Problem sending exception: " + e.getMessage() + JSONMessage), (Throwable)e);
        }
        catch (ConnectionClosedException e2) {
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)"Connection closed when sending exception", (Throwable)e2);
            }
        }
    }
    
    public static void initCleanupRoutine() {
        RMIWebSocket.serverAuthTokens.put(HazelcastSingleton.getNodeId(), new ArrayList());
        Server.server.getScheduler().scheduleAtFixedRate(new SessionCleanup(), RMIWebSocket.ONE_MINUTE, RMIWebSocket.ONE_MINUTE, TimeUnit.MILLISECONDS);
    }
    
    public void onMessage(final String jsonMsg) {
        if (!this.closed) {
            String callbackIndex = null;
            final List<String> paramStrList = new ArrayList<String>();
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)("Received message: " + jsonMsg));
            }
            try {
                final JsonNode rootNode = (JsonNode)this.mapper.readValue(jsonMsg, (Class)JsonNode.class);
                if (rootNode == null) {
                    throw new RMIWebSocketException("The sent message is not JSON format - " + jsonMsg);
                }
                callbackIndex = this.getJsonNodeValue(rootNode, "callbackIndex", jsonMsg);
                final String className = this.getJsonNodeValue(rootNode, "class", jsonMsg);
                final String methodName = this.getJsonNodeValue(rootNode, "method", jsonMsg);
                final JsonNode paramsNode = rootNode.get("params");
                if (paramsNode != null) {
                    final Iterator<JsonNode> params = (Iterator<JsonNode>)paramsNode.elements();
                    while (params.hasNext()) {
                        final JsonNode param = params.next();
                        final String paramStr = this.mapper.writeValueAsString((Object)param);
                        paramStrList.add(paramStr);
                    }
                }
                if ("this".equalsIgnoreCase(className)) {
                    this.handleSpecialMessages(className, methodName, callbackIndex, paramStrList);
                    return;
                }
                final Map<String, Method> methodMap = RMIWebSocket.classMap.get(className);
                if (methodMap == null) {
                    throw new RMIWebSocketException("The class '" + className + " has not been registered. Valid classes are: " + RMIWebSocket.classMap.keySet());
                }
                final String methodKey = methodName + ":" + paramStrList.size();
                final Method method = methodMap.get(methodKey);
                if (method == null) {
                    throw new RMIWebSocketException("The method " + methodKey + " with " + paramStrList.size() + " parameters has not been registered for class " + className + ". Valid methods are " + methodMap.keySet());
                }
                final Object[] paramObjs = new Object[paramStrList.size()];
                final Class<?>[] paramTypes = method.getParameterTypes();
                for (int i = 0; i < paramTypes.length; ++i) {
                    paramObjs[i] = this.mapper.readValue((String)paramStrList.get(i), (Class)paramTypes[i]);
                }
                final Object invokeOn = RMIWebSocket.objectMap.get(className);
                Object result;
                try {
                    if (RMIWebSocket.logger.isDebugEnabled()) {
                        RMIWebSocket.logger.debug((Object)("Calling method " + method + " on " + invokeOn + " with params " + Arrays.toString(paramObjs)));
                    }
                    if (method.getName().equalsIgnoreCase("getallhds")) {
                        result = method.invoke(invokeOn, paramObjs);
                    }
                    else {
                        result = method.invoke(invokeOn, paramObjs);
                    }
                    if (RMIWebSocket.logger.isDebugEnabled() && RMIWebSocket.logger.isDebugEnabled()) {
                        RMIWebSocket.logger.debug((Object)("Result from method " + method + " on " + invokeOn + " is " + result));
                    }
                }
                catch (InvocationTargetException e) {
                    Throwable targetException;
                    for (targetException = e.getCause(); targetException.getCause() != null; targetException = targetException.getCause()) {}
                    final String message = targetException.getMessage();
                    throw new RMIWebSocketException(message, targetException);
                }
                catch (Exception e2) {
                    final String message2 = e2.getMessage();
                    throw new RMIWebSocketException(message2, e2);
                }
                if (result == null) {
                    result = "Completed.";
                }
                this.call(true, callbackIndex, result);
            }
            catch (Exception e3) {
                this.handleMessageException(callbackIndex, e3, jsonMsg);
            }
        }
    }
    
    public static void cleanupLeftUsers(final String serverUUIDString) {
        if (serverUUIDString == null) {
            RMIWebSocket.logger.debug((Object)"Cleanup left user is null");
            return;
        }
        final UUID serverUUID = new UUID(serverUUIDString);
        if (RMIWebSocket.serverAuthTokens != null) {
            final List<AuthToken> authTokens = (List<AuthToken>)RMIWebSocket.serverAuthTokens.remove((Object)serverUUID);
            if (authTokens != null) {
                for (final AuthToken authToken : authTokens) {
                    if (authToken != null && HSecurityManager.get().isAuthenticated(authToken)) {
                        HSecurityManager.get().logout(authToken);
                    }
                }
            }
        }
    }
    
    private void handleAlertSubscription(final String callback, final UUID id, final AuthToken token) throws Exception {
        String userId = null;
        try {
            userId = HSecurityManager.getAutheticatedUserName(token);
        }
        catch (SecurityException e) {
            throw new RMIWebSocketException("Token " + token + " does not belong to authenticated user");
        }
        this.addToSessionList(this.userAuthToken = token);
        RMIWebSocket.keepAlive.put(this.userAuthToken, System.currentTimeMillis());
        Set<MetaInfo.Target> subs = (Set<MetaInfo.Target>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.TARGET, token);
        if (subs == null) {
            subs = new HashSet<MetaInfo.Target>();
        }
        final Map<String, UUID> myAlertSubscriptions = new HashMap<String, UUID>();
        this.alertSubscriptionIds.put(id, myAlertSubscriptions);
        for (final MetaInfo.Target sub : subs) {
            if (sub.isSubscription() && sub.adapterClassName.equals("com.datasphere.proc.WebAlertAdapter")) {
                final UUID subId = this.subscribeToQueue(callback, sub.getChannelName(), false, token);
                myAlertSubscriptions.put(sub.getChannelName(), subId);
                if (!RMIWebSocket.logger.isInfoEnabled()) {
                    continue;
                }
                RMIWebSocket.logger.info((Object)("Subscribe to " + sub.getName() + " for " + userId + "channel name:" + sub.getChannelName()));
            }
        }
    }
    
    private void handleAlertUnsubscription(final String callback, final UUID id, final AuthToken token) throws RMIWebSocketException, MetaDataRepositoryException {
        String userId = null;
        try {
            userId = HSecurityManager.getAutheticatedUserName(token);
        }
        catch (SecurityException e) {
            throw new RMIWebSocketException("Token " + token + " does not belong to authenticated user");
        }
        final Set<MetaInfo.Target> subs = (Set<MetaInfo.Target>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.TARGET, token);
        final Map<String, UUID> myAlertSubscriptions = this.alertSubscriptionIds.get(id);
        for (final MetaInfo.Target sub : subs) {
            if (myAlertSubscriptions != null) {
                final UUID subId = myAlertSubscriptions.get(sub.getChannelName());
                if (subId != null) {
                    this.unsubscribeFromQueue(callback, sub.getChannelName(), subId.getUUIDString(), false, token);
                }
            }
            if (RMIWebSocket.logger.isInfoEnabled()) {
                RMIWebSocket.logger.info((Object)("Unsubscribe from  " + sub.getChannelName() + " for " + userId));
            }
        }
    }
    
    private UUID subscribeToQueue(final String callbackIndex, final String queueName, final AuthToken token) throws Exception {
        return this.subscribeToQueue(callbackIndex, queueName, true, token);
    }
    
    private UUID subscribeToQueue(final String callbackIndex, final String queueName, final boolean responseExpected, final AuthToken token) throws Exception {
        if (RMIWebSocket.logger.isInfoEnabled()) {
            RMIWebSocket.logger.info((Object)("Subscribe to Queue " + queueName + " with callback " + callbackIndex));
        }
        UUID id = null;
        synchronized (this.listenerMap) {
            for (final Map.Entry<String, HQueue.Listener> entry : this.listenerMap.entrySet()) {
                final QueueSender s = (QueueSender)entry.getValue();
                if (s.queueName.equals(queueName)) {
                    try {
                        this.call(true, callbackIndex, s.id.getUUIDString(), "Subscribed");
                    }
                    catch (RMIWebSocketException e3) {
                        RMIWebSocket.logger.error((Object)"Problem sending subscribe ID to client");
                    }
                    catch (ConnectionClosedException e) {
                        if (RMIWebSocket.logger.isDebugEnabled()) {
                            RMIWebSocket.logger.debug((Object)"Connection closed on subscribe", (Throwable)e);
                        }
                    }
                    return s.id;
                }
            }
            if (RMIWebSocket.logger.isInfoEnabled()) {
                RMIWebSocket.logger.info((Object)("getting wa-queue for queueName : " + queueName));
            }
            if (queueName == null || queueName.isEmpty()) {
                RMIWebSocket.logger.warn((Object)("null or empty queue name is passed. queueName:" + queueName));
            }
            else {
                final HQueue queue = HQueue.getQueue(queueName);
                id = new UUID(System.currentTimeMillis());
                final HQueue.Listener listener = new QueueSender(id, queueName, queue, callbackIndex);
                queue.subscribe(listener);
                this.listenerMap.put(id.getUUIDString(), listener);
                if (RMIWebSocket.logger.isInfoEnabled()) {
                    RMIWebSocket.logger.info((Object)("Subscribe to Queue " + queueName + " has id " + id.getUUIDString()));
                }
                if (responseExpected) {
                    try {
                        this.call(true, callbackIndex, id.getUUIDString(), "Subscribed to " + queueName);
                    }
                    catch (RMIWebSocketException e4) {
                        RMIWebSocket.logger.error((Object)"Problem sending subscribe ID to client");
                    }
                    catch (ConnectionClosedException e2) {
                        if (RMIWebSocket.logger.isDebugEnabled()) {
                            RMIWebSocket.logger.debug((Object)"Connection closed on subscribe", (Throwable)e2);
                        }
                    }
                }
            }
        }
        if ("alertQueue".equals(queueName)) {
            this.handleAlertSubscription(callbackIndex, id, token);
        }
        return id;
    }
    
    private void unsubscribeFromQueue(final String callbackIndex, final String queueName, final String id, final AuthToken token) throws RMIWebSocketException, MetaDataRepositoryException {
        this.unsubscribeFromQueue(callbackIndex, queueName, id, true, token);
    }
    
    private void unsubscribeFromQueue(final String callbackIndex, final String queueName, final String id, final boolean responseExpected, final AuthToken token) throws RMIWebSocketException, MetaDataRepositoryException {
        if (RMIWebSocket.logger.isInfoEnabled()) {
            RMIWebSocket.logger.info((Object)("Unsubscribe from Queue " + queueName + " with callback " + callbackIndex + " id " + id));
        }
        if ("alertQueue".equals(queueName)) {
            this.handleAlertUnsubscription(callbackIndex, new UUID(id), token);
        }
        synchronized (this.listenerMap) {
            final HQueue queue = HQueue.getQueue(queueName);
            final HQueue.Listener listener = this.listenerMap.get(id);
            if (listener == null) {
                throw new RMIWebSocketException("Could not locate existing queue " + queueName + " subscription with id " + id);
            }
            queue.unsubscribe(listener);
            this.listenerMap.remove(id);
            if (responseExpected) {
                try {
                    this.call(true, callbackIndex, id, "Unsubscribed from " + queueName);
                }
                catch (RMIWebSocketException e2) {
                    RMIWebSocket.logger.error((Object)"Problem sending subscribe ID to client");
                }
                catch (ConnectionClosedException e) {
                    if (RMIWebSocket.logger.isDebugEnabled()) {
                        RMIWebSocket.logger.debug((Object)"Connection closed on unsubscribe", (Throwable)e);
                    }
                }
            }
        }
    }
    
    private void addToSessionList(final AuthToken userAuthToken) {
        try {
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)("I am locking for user - " + userAuthToken));
            }
            RMIWebSocket.serverAuthTokens.lock(userAuthToken);
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)("Locked for user - " + userAuthToken));
            }
            final List authTokens = (List)RMIWebSocket.serverAuthTokens.get((Object)HazelcastSingleton.getNodeId());
            authTokens.add(userAuthToken);
            RMIWebSocket.serverAuthTokens.put(HazelcastSingleton.getNodeId(), authTokens);
        }
        catch (Exception e) {
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)e.getMessage(), (Throwable)e);
            }
        }
        finally {
            if (RMIWebSocket.logger.isDebugEnabled()) {
                RMIWebSocket.logger.debug((Object)("I am unlocking for user - " + userAuthToken));
            }
            RMIWebSocket.serverAuthTokens.unlock(userAuthToken);
        }
    }
    
    private void ping(final String callbackIndex) throws Exception {
        if (this.userAuthToken != null) {
            RMIWebSocket.keepAlive.put(this.userAuthToken, System.currentTimeMillis());
        }
        this.call(true, callbackIndex, "Pong");
    }
    
    private void list(String callbackIndex) throws RMIWebSocketException {
        if (callbackIndex == null) {
            callbackIndex = "callbackF";
        }
        if (this.closed) {
            throw new RMIWebSocketException("WebSocket connection is already closed");
        }
        if (!this.connection.isOpen()) {
            throw new RMIWebSocketException("WebSocket connection already closed by client");
        }
        final ObjectNode rootNode = this.mapper.createObjectNode();
        rootNode.put("success", true);
        rootNode.put("callbackIndex", callbackIndex);
        final ArrayNode returnParamsArray = rootNode.putArray("params");
        final ArrayNode classesArray = this.mapper.createArrayNode();
        returnParamsArray.add((JsonNode)classesArray);
        for (final Map.Entry<String, Map<String, Method>> classEntry : RMIWebSocket.classMap.entrySet()) {
            final ObjectNode classNode = this.mapper.createObjectNode();
            classNode.put("class", (String)classEntry.getKey());
            final ArrayNode methodsArray = classNode.putArray("method-list");
            for (final Map.Entry<String, Method> methodEntry : classEntry.getValue().entrySet()) {
                final ObjectNode methodNode = this.mapper.createObjectNode();
                final Method method = methodEntry.getValue();
                methodNode.put("method", method.getName());
                final ArrayNode paramsArray = methodNode.putArray("params-list");
                final Class<?>[] parameterTypes;
                final Class<?>[] paramTypes = parameterTypes = method.getParameterTypes();
                for (final Class<?> paramType : parameterTypes) {
                    paramsArray.add(paramType.getCanonicalName());
                }
                methodNode.put("returns", method.getReturnType().getCanonicalName());
                methodsArray.add((JsonNode)methodNode);
            }
            classesArray.add((JsonNode)classNode);
        }
        String msg;
        try {
            msg = this.mapper.writeValueAsString((Object)rootNode);
        }
        catch (IOException e) {
            throw new RMIWebSocketException("Unable to send serialize JsonNode " + rootNode, e);
        }
        try {
            this.connection.sendMessage(msg);
        }
        catch (IOException e) {
            throw new RMIWebSocketException("Unable to send message " + msg.substring(0, 200) + ((msg.length() > 200) ? "..." : ""));
        }
    }
    
    private void callStart() throws ConnectionClosedException {
        if (this.closed) {
            throw new ConnectionClosedException("WebSocket connection is already closed");
        }
        if (!this.connection.isOpen()) {
            throw new ConnectionClosedException("WebSocket connection already closed by client");
        }
    }
    
    private void callFinish(final ObjectNode rootNode, final String reference) throws RMIWebSocketException {
        String msg;
        try {
            msg = this.mapper.writeValueAsString((Object)rootNode);
        }
        catch (IOException e) {
            throw new RMIWebSocketException("Unable to send serialize JsonNode " + rootNode, e);
        }
        try {
            if (RMIWebSocket.logger.isInfoEnabled()) {
                String outMsg = msg;
                if (msg.length() > 200) {
                    outMsg = msg.substring(0, 200) + "...";
                }
                if (RMIWebSocket.logger.isDebugEnabled()) {
                    RMIWebSocket.logger.debug((Object)("Sending callback: " + reference + " -> " + outMsg));
                }
            }
            this.connection.sendMessage(msg);
        }
        catch (IOException e) {
            throw new RMIWebSocketException("Unable to send message " + msg.substring(0, 200) + ((msg.length() > 200) ? "..." : ""));
        }
    }
    
    public synchronized void call(final boolean success, final String callbackIndex, final Object... params) throws RMIWebSocketException, ConnectionClosedException {
        this.callStart();
        final ObjectNode rootNode = this.mapper.createObjectNode();
        rootNode.put("success", success);
        rootNode.put("callbackIndex", callbackIndex);
        final ArrayNode paramArray = rootNode.putArray("params");
        for (final Object param : params) {
            this.addToArray(paramArray, param, this.mapper);
        }
        this.callFinish(rootNode, callbackIndex);
    }
    
    private void addToArray(final ArrayNode arr, final Object param, final ObjectMapper mapper) throws RMIWebSocketException {
        if (param == null) {
            arr.addNull();
        }
        else if (param instanceof JsonNode) {
            arr.add((JsonNode)param);
        }
        else if (param instanceof ObjectNode) {
            arr.add((JsonNode)param);
        }
        else if (param instanceof Integer) {
            arr.add((Integer)param);
        }
        else if (param instanceof String) {
            arr.add((String)param);
        }
        else if (param instanceof Float) {
            arr.add((Float)param);
        }
        else if (param instanceof Double) {
            arr.add((Double)param);
        }
        else if (param instanceof BigDecimal) {
            arr.add((BigDecimal)param);
        }
        else if (param instanceof Map) {
            String jsonRepr;
            try {
                jsonRepr = mapper.writeValueAsString((Object)param);
            }
            catch (Exception e) {
                throw new RMIWebSocketException("Unable to serialize map " + param, e);
            }
            try {
                arr.add((JsonNode)mapper.readValue(jsonRepr, (Class)JsonNode.class));
            }
            catch (Exception e) {
                throw new RMIWebSocketException("Unable to de-serialize map contents " + jsonRepr, e);
            }
        }
        else if (param.getClass().isArray()) {
            final ArrayNode childArr = arr.addArray();
            Object[] paramArr;
            try {
                paramArr = (Object[])param;
            }
            catch (ClassCastException e2) {
                throw new RMIWebSocketException("Arrays of unboxed types like int, double are not supported. Use Integer[] and Double[]", e2);
            }
            for (final Object paramElem : paramArr) {
                this.addToArray(childArr, paramElem, mapper);
            }
        }
        else {
            arr.addPOJO(param);
        }
    }
    
    static {
        RMIWebSocket.logger = Logger.getLogger((Class)RMIWebSocket.class);
        RMIWebSocket.TEN_MINUTES = 600000L;
        RMIWebSocket.ONE_MINUTE = 60000L;
        RMIWebSocket.serverAuthTokens = HazelcastSingleton.get().getMap("#serverAuthTokens");
        RMIWebSocket.keepAlive = new ConcurrentHashMap<AuthToken, Long>();
        RMIWebSocket.classMap = new HashMap<String, Map<String, Method>>();
        RMIWebSocket.objectMap = new HashMap<String, Object>();
    }
    
    public static class Handler extends WebSocketHandler
    {
        WebSocket socket;
        
        public WebSocket doWebSocketConnect(final HttpServletRequest request, final String protocol) {
            return this.socket = (WebSocket)new RMIWebSocket();
        }
    }
    
    public static class SessionCleanup extends TimerTask
    {
        @Override
        public void run() {
            final long currentTime = System.currentTimeMillis();
            if (RMIWebSocket.keepAlive != null) {
                for (final AuthToken userAuthToken : RMIWebSocket.keepAlive.keySet()) {
                    final long timeLeft = currentTime - RMIWebSocket.keepAlive.get(userAuthToken);
                    if (RMIWebSocket.logger.isDebugEnabled()) {
                        RMIWebSocket.logger.debug((Object)("For user: " + userAuthToken + " - time left: " + timeLeft));
                    }
                    if (timeLeft > RMIWebSocket.TEN_MINUTES) {
                        if (userAuthToken != null) {
                            HSecurityManager.get().logout(userAuthToken);
                        }
                        RMIWebSocket.keepAlive.remove(userAuthToken);
                    }
                }
            }
        }
    }
    
    public class QueueSender implements HQueue.Listener
    {
        public String queueName;
        public HQueue queue;
        public UUID id;
        public String callbackIndex;
        
        public QueueSender(final UUID id, final String queueName, final HQueue queue, final String callbackIndex) {
            this.id = id;
            this.queueName = queueName;
            this.queue = queue;
            this.callbackIndex = callbackIndex;
        }
        
        private void unsubscribeLater() {
            final Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    QueueSender.this.queue.unsubscribe(QueueSender.this);
                    String removeId = null;
                    synchronized (RMIWebSocket.this.listenerMap) {
                        for (final Map.Entry<String, HQueue.Listener> entry : RMIWebSocket.this.listenerMap.entrySet()) {
                            final QueueSender s = (QueueSender)entry.getValue();
                            if (s == QueueSender.this) {
                                removeId = s.id.getUUIDString();
                            }
                        }
                        if (removeId != null) {
                            RMIWebSocket.this.listenerMap.remove(removeId);
                        }
                    }
                }
            }, 100L);
        }
        
        @Override
        public void onItem(final Object item) {
            try {
                RMIWebSocket.this.call(true, this.callbackIndex, item);
            }
            catch (RMIWebSocketException e) {
                RMIWebSocket.logger.error((Object)"Could not send message to client", (Throwable)e);
                this.unsubscribeLater();
            }
            catch (ConnectionClosedException e2) {
                if (RMIWebSocket.logger.isDebugEnabled()) {
                    RMIWebSocket.logger.debug((Object)"Connection closed when sending message", (Throwable)e2);
                }
                this.unsubscribeLater();
            }
        }
    }
}
