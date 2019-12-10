package com.datasphere.tungsten;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.MappedByteBuffer;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.datasphere.cache.CacheConfiguration;
import com.datasphere.cache.CacheManager;
import com.datasphere.cache.CachingProvider;
import com.datasphere.cache.ICache;
import com.datasphere.classloading.DSSClassLoader;
import com.datasphere.classloading.HDLoader;
import com.datasphere.distribution.HQueue;
import com.datasphere.event.QueryResultEvent;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastMemberAddressProvider;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDClientOps;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.proc.events.ShowStreamEvent;
import com.datasphere.recovery.AppCheckpointSummary;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.ConsoleReader;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.NodeStartUp;
import com.datasphere.runtime.PartitionManager;
import com.datasphere.runtime.ShowStreamManager;
import com.datasphere.runtime.Version;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.ConnectStmt;
import com.datasphere.runtime.compiler.stmts.CreatePropertySetStmt;
import com.datasphere.runtime.compiler.stmts.CreatePropertyVariableStmt;
import com.datasphere.runtime.compiler.stmts.CreateUserStmt;
import com.datasphere.runtime.compiler.stmts.DumpStmt;
import com.datasphere.runtime.compiler.stmts.LoadFileStmt;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.SessionInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.usagemetrics.tungsten.Usage;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Tungsten
{
    private static Logger logger;
    public static AtomicBoolean isAdhocRunning;
    public static AtomicBoolean isQueryDumpOn;
    public static AuthToken session_id;
    public static MetaInfo.User currUserMetaInfo;
    public static DateTimeZone userTimeZone;
    public static boolean quit;
    public static PrintFormat currentFormat;
    protected static String commandLineFormat;
    protected static Context ctx;
    private static String HD_LOG;
    private static Map<Keywords, String> enumMap;
    private static String SPOOL_ERROR_MSG;
    private static File file;
    private static boolean insinglequote;
    private static boolean indoublequote;
    private static MetaInfo.Query query;
    private static String comments;
    private static boolean isShuttingDown;
    private static MDClientOps clientOps;
    private static AtomicInteger NUM_OF_ERRORS;
    private static boolean commandLoggingEnabled;
    private boolean isBatch;
    private static HQueue sessionQueue;
    private static HQueue.Listener queuelistener;
    private static SimpleWebSocketClient tungsteClient;
    static MappedByteBuffer out;
    static int SIZE;
    public static Stream showStream;
    public static final String CLASS = "class";
    public static final String METHOD = "method";
    public static final String PARAMS = "params";
    public static final String CALLBACK = "callback";
    public static final String TYPE = "Tungsten";
    
    public static void setSessionQueue(final HQueue sessionQueue) {
        Tungsten.sessionQueue = sessionQueue;
    }
    
    public static void setQueuelistener(final HQueue.Listener queuelistener) {
        Tungsten.queuelistener = queuelistener;
    }
    
    public Tungsten() {
        this.isBatch = false;
        Tungsten.enumMap.put(Keywords.HELP, "[Lists this help menu]\n            'help <objectType>' - Gives example of object type e.g 'help type'");
        Tungsten.enumMap.put(Keywords.PREV, "[Prints and repeats the previous command]");
        Tungsten.enumMap.put(Keywords.HISTORY, "[Prints all commands in this session]\n            'history clear' - Deletes the history\n            'history x' - Executes command at number x");
        Tungsten.enumMap.put(Keywords.GO, "[End batch scripts after batchmode]");
        Tungsten.enumMap.put(Keywords.CONNECT, "usage: CONNECT username password [TO CLUSTER clustername]\n           connects as the user to the current cluster [or the specified cluster]");
        Tungsten.enumMap.put(Keywords.SET, "[Set options such as: batchmode...etc]");
        Tungsten.enumMap.put(Keywords.SHOW, "[Show stream e.g. \"show s1\"]");
        Tungsten.enumMap.put(Keywords.QUIT, "[Quit]");
        Tungsten.enumMap.put(Keywords.EXIT, "[Quit]");
        Tungsten.enumMap.put(Keywords.LIST, "[List metadata objects by type e.g. \"list types\"]");
        Tungsten.enumMap.put(Keywords.DESCRIBE, "[Describe metadata objects by given name e.g. \"describe typename\"]");
        Tungsten.enumMap.put(Keywords.MONITOR, "usage: monitor command [id] [-n info-names] [-v]\n         command: cluster, entity, follow, count, clear\n              id: a three-part string in the format APPNAME.ENTITY-TYPE.ENTITY-NAME\n   -n info-names: a list of names separated by commas (but not spaces!)\n                  ex: -n input-total,output-total,processed\n              -v: verbose, prints extra details");
    }
    
    public static void argsController(final String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("-batchfile")) {
                if (args.length == i + 1) {
                    printf("\n No Filename given!");
                    return;
                }
                final String scriptFilename = args[i + 1];
                final File fname = new File(scriptFilename);
                if (!fname.exists()) {
                    printf("\nFile " + scriptFilename + " does not exist!\n");
                    return;
                }
                FileReader in = null;
                try {
                    in = new FileReader(scriptFilename);
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                final StringBuilder contents = new StringBuilder();
                final char[] buffer = new char[4096];
                int read = 0;
                do {
                    contents.append(buffer, 0, read);
                    try {
                        read = in.read(buffer);
                    }
                    catch (IOException e2) {
                        e2.printStackTrace();
                    }
                } while (read >= 0);
                try {
                    in.close();
                }
                catch (IOException ex) {}
                final String stripped = contents.toString().replaceAll("(?m)^((?:(?!--|').|'(?:''|[^'])*')*)--.*$", "$1");
                process(stripped);
            }
        }
    }
    
    static UUID userAuth() throws IOException {
        String uname = System.getProperty("com.datasphere.config.username");
        String password = System.getProperty("com.datasphere.config.password");
        for (int i = 0; i < 3; ++i) {
            while (uname == null || uname.isEmpty()) {
                uname = ConsoleReader.readLine("Username : ");
            }
            while (password == null || password.isEmpty()) {
                password = ConsoleReader.readPassword("Password (for " + uname + "): ");
            }
            try {
                final String clientId = HazelcastSingleton.get().getLocalEndpoint().getUuid();
                Tungsten.session_id = Tungsten.clientOps.authenticate(uname, password, clientId, "Tungsten");
                if (Tungsten.session_id != null) {
                    Tungsten.currUserMetaInfo = (MetaInfo.User)Tungsten.clientOps.getMetaObjectByName(EntityType.USER, "Global", uname, null, Tungsten.session_id);
                    try {
                        final TimeZone jtz = TimeZone.getTimeZone(Tungsten.currUserMetaInfo.getUserTimeZone());
                        Tungsten.userTimeZone = (Tungsten.currUserMetaInfo.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz));
                    }
                    catch (Exception e2) {
                        printf("Unable to use user's timezone - " + Tungsten.currUserMetaInfo.getUserTimeZone() + ", defaulting to local timezone");
                    }
                    return (UUID)Tungsten.session_id;
                }
            }
            catch (Exception e) {
                Tungsten.logger.error((Object)e);
                printf("Invalid username or password.\n");
                password = (uname = null);
            }
        }
        System.exit(Tungsten.NUM_OF_ERRORS.get());
        return null;
    }
    
    public static void printf(final String toPrint) {
        synchronized (System.out) {
            if (System.console() != null) {
                final String escapedToPrint = toPrint.replace("%", "%%");
                System.console().printf(escapedToPrint, new Object[0]);
            }
            else {
                System.out.print(toPrint);
            }
            System.out.flush();
        }
    }
    
    private static void shutDown() {
        if (Tungsten.ctx != null) {
            Tungsten.isShuttingDown = true;
            Context.shutdown("Tungsten ...");
        }
    }
    
    private static void initialize() throws Exception {
        String cName = System.getProperty("com.datasphere.config.clusterName");
        if (cName == null || cName.isEmpty()) {
            cName = NodeStartUp.getClusterName();
            System.setProperty("com.datasphere.config.clusterName", cName);
        }
        try {
            final HazelcastInstance HZ = HazelcastSingleton.get(cName, HazelcastSingleton.HDClusterMemberType.CONSOLENODE);
            Tungsten.clientOps = MDClientOps.getINSTANCE();
            HZ.getLifecycleService().addLifecycleListener((LifecycleListener)new LifecycleListener() {
                public void stateChanged(final LifecycleEvent event) {
                    if (event.getState().equals((Object)LifecycleEvent.LifecycleState.SHUTTING_DOWN)) {
                        Tungsten.ctx = null;
                        if (!Tungsten.isShuttingDown) {
                            System.out.println("\nNo available servers in cluster, shutting down.");
                            System.exit(Tungsten.NUM_OF_ERRORS.get());
                        }
                    }
                }
            });
            final DSSClassLoader scl = (DSSClassLoader)Thread.currentThread().getContextClassLoader();
            try {
                scl.scanModulePath();
            }
            catch (IOException ioe) {
                Tungsten.logger.warn((Object)("Unable to load modules from modulepath : " + ioe.getMessage()), (Throwable)ioe);
            }
            HDLoader.get();
            printf(".");
            printf(".");
            printf(".");
            try {
                HSecurityManager.setFirstTime(false);
                printf(".");
            }
            catch (SecurityException e) {
                if (Tungsten.logger.isDebugEnabled()) {
                    Tungsten.logger.debug((Object)e.getMessage());
                }
            }
            printf("connected.\n");
            userAuth();
            HazelcastSingleton.activeClusterMembers.put(HazelcastSingleton.getNodeId(), HazelcastSingleton.get().getLocalEndpoint());
            initShowStream();
            setSessionQueue(HQueue.getQueue("consoleQueue" + Tungsten.session_id));
            setQueuelistener(new HQueue.Listener() {
                @Override
                public void onItem(final Object item) {
                    if (Tungsten.isAdhocRunning.get()) {
                        Tungsten.prettyPrintEvent(item);
                    }
                    else if (item instanceof JsonNode && Tungsten.isQueryDumpOn.get()) {
                        final JsonNode jNode = (JsonNode)item;
                        System.out.print("Source:" + jNode.get("datasource"));
                        System.out.print(",IO:" + jNode.get("IO"));
                        System.out.print(",Action:" + jNode.get("type"));
                        System.out.print(",Server:" + jNode.get("serverid"));
                        System.out.println(",Data:" + jNode.get("data"));
                    }
                }
            });
            getSessionQueue().subscribeForTungsten(getQueuelistener());
        }
        catch (Exception e2) {
            Tungsten.logger.warn((Object)e2.getMessage());
            printf("No Server found in the cluster : " + cName + ". \n");
            System.exit(-1);
        }
    }
    
    public static void prettyPrintEvent(final Object e) {
        if (e instanceof TaskEvent) {
            if (((TaskEvent)e).getQueryID() != null && Tungsten.query.getUuid() != null && !((TaskEvent)e).getQueryID().equals((Object)Tungsten.query.getUuid())) {
                if (Tungsten.logger.isInfoEnabled()) {
                    Tungsten.logger.info((Object)"Old query result ignored.");
                }
                return;
            }
            final IBatch<DARecord> DARecordIBatch = (IBatch<DARecord>)((TaskEvent)e).batch();
            if (Tungsten.currentFormat == PrintFormat.ROW_FORMAT) {
                CluiMonitorView.printTableTitle("Query Results");
                if (((TaskEvent)e).batch() != null && !((TaskEvent)e).batch().isEmpty() && ((DARecord)DARecordIBatch.first()).data != null) {
                    printTableRow(((QueryResultEvent)((DARecord)DARecordIBatch.first()).data).fieldsInfo);
                    CluiMonitorView.printTableDivider();
                }
            }
            for (final DARecord b : DARecordIBatch) {
                if (Tungsten.currentFormat == PrintFormat.ROW_FORMAT) {
                    rowPrintData((QueryResultEvent)b.data);
                }
                else {
                    if (Tungsten.currentFormat != PrintFormat.JSON) {
                        continue;
                    }
                    multiPrint("[\n");
                    printQueryEvent(b.data);
                    multiPrint("]\n");
                }
            }
        }
        else if (e instanceof QueryResultEvent) {
            multiPrint("[\n");
            printQueryEvent(e);
            multiPrint("]\n");
        }
        else if (e instanceof ShowStreamEvent) {
            multiPrint("[\n");
            multiPrint(((ShowStreamEvent)e).event.toString());
            multiPrint("]\n");
        }
    }
    
    private static void rowPrintData(final QueryResultEvent qre) {
        printTableRow(qre.getDataPoints());
        CluiMonitorView.printTableDivider();
    }
    
    private static void printTableRow(final Object[] columnNames) {
        final StringBuilder sb = new StringBuilder();
        final int MIN_COL;
        int TMP_WIDTH = MIN_COL = 132 / columnNames.length;
        int sum = 0;
        for (int i = 0; i < columnNames.length; ++i) {
            if (i + 1 == columnNames.length) {
                final int delta = TMP_WIDTH = 132 - sum;
            }
            sum += TMP_WIDTH;
            sb.append("%-").append(TMP_WIDTH).append("s");
        }
        final String formatter = "= " + (Object)sb + "=%n";
        for (int j = 0; j < columnNames.length; ++j) {
            if (columnNames[j] != null && columnNames[j].toString().length() > MIN_COL) {
                final String columnString = columnNames[j].toString();
                columnNames[j] = columnNames[j].toString().substring(0, MIN_COL / 2 - 2) + "~" + columnNames[j].toString().substring(columnString.length() - MIN_COL / 2 + 2, columnString.length()).trim();
            }
        }
        System.out.format(formatter, columnNames);
    }
    
    private static void printQueryEvent(final Object e) {
        if (e instanceof QueryResultEvent) {
            final QueryResultEvent qre = (QueryResultEvent)e;
            int i = 0;
            final Object[] payload = qre.getPayload();
            final String[] fieldsInfo;
            final String[] fieldNames = fieldsInfo = qre.getFieldsInfo();
            for (final String fieldName : fieldsInfo) {
                if (payload[i] == null || payload[i].getClass() == null) {
                    multiPrint("   " + fieldName + " = null \n");
                }
                else if (payload[i].getClass().equals(byte[].class)) {
                    multiPrint("   " + fieldName + " = " + Arrays.toString((byte[])payload[i]) + "\n");
                }
                else if (payload[i].getClass().isArray()) {
                    multiPrint("   " + fieldName + " = " + Arrays.toString((Object[])payload[i]) + "\n");
                }
                else if (payload[i].getClass().equals(DateTime.class) && Tungsten.userTimeZone != null) {
                    multiPrint("   " + fieldName + " = " + new DateTime(payload[i], Tungsten.userTimeZone) + "\n");
                }
                else {
                    multiPrint("   " + fieldName + " = " + payload[i] + "\n");
                }
                ++i;
            }
        }
    }
    
    protected static MetaInfo.Type getTypeForCLass(final Class<?> clazz) throws MetaDataRepositoryException {
        final String typeName = "Global." + clazz.getSimpleName();
        MetaInfo.Type type;
        try {
            type = (MetaInfo.Type)Tungsten.clientOps.getMetaObjectByName(EntityType.TYPE, "Global", clazz.getSimpleName(), null, HSecurityManager.TOKEN);
        }
        catch (Exception e) {
            if (Tungsten.logger.isInfoEnabled()) {
                Tungsten.logger.info((Object)e.getLocalizedMessage());
            }
            return null;
        }
        if (type == null) {
            final Map<String, String> fields = new LinkedHashMap<String, String>();
            final Field[] declaredFields;
            final Field[] cFields = declaredFields = clazz.getDeclaredFields();
            for (final Field f : declaredFields) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            type = new MetaInfo.Type();
            type.construct(typeName, MetaInfo.GlobalNamespace, clazz.getName(), fields, null, false);
        }
        return type;
    }
    
    public static void initShowStream() {
        try {
            if (Tungsten.logger.isInfoEnabled()) {
                Tungsten.logger.info((Object)"Creating SHOW Stream");
            }
            final MetaInfo.Type dataType = getTypeForCLass(ShowStreamEvent.class);
            final MetaInfo.Stream streamMetaObj = new MetaInfo.Stream();
            streamMetaObj.construct("showStream", ShowStreamManager.ShowStreamUUID, MetaInfo.GlobalNamespace, dataType.uuid, null, null, null);
            (Tungsten.showStream = new Stream(streamMetaObj, null, null)).start();
            if (Tungsten.logger.isDebugEnabled()) {
                Tungsten.logger.debug((Object)"exception stream, control stream are created");
            }
        }
        catch (Exception se) {
            Tungsten.logger.error((Object)("error" + se));
            se.printStackTrace();
        }
    }
    
    private static void multiPrint(final String msg) {
        synchronized (System.out) {
            if (System.console() != null) {
                final String escapedToPrint = msg.replace("%", "%%");
                System.console().printf(escapedToPrint, new Object[0]);
            }
            else {
                System.out.print(msg);
            }
            System.out.flush();
        }
        if (SpoolFile.fileName != null && SpoolFile.spoolMode.get()) {
            if (SpoolFile.writer == null) {
                try {
                    SpoolFile.writer = new PrintWriter(SpoolFile.fileName, "UTF-8");
                }
                catch (FileNotFoundException | UnsupportedEncodingException ex2) {
                    Tungsten.logger.error((Object)ex2.getMessage());
                }
            }
            if (SpoolFile.writer != null) {
                SpoolFile.writer.print(msg);
            }
        }
    }
    
    public static void checkAndCleanupAdhoc(final Boolean stopShowStream) {
        try {
            if (stopShowStream) {
                Tungsten.showStream.stop();
            }
            getSessionQueue().unsubscribe(getQueuelistener());
        }
        catch (Exception e) {
            if (Tungsten.logger.isInfoEnabled()) {
                Tungsten.logger.info((Object)e.getMessage());
            }
        }
    }
    
    public static void main(final String[] args) throws Exception {
        printf("Welcome to the DSS Tungsten Command Line - " + Version.getVersionString() + "\n");
        initialize();
        final IMap<UUID, UUID> nodeIDToAuthToken = HazelcastSingleton.get().getMap("#nodeIDToAuthToken");
        nodeIDToAuthToken.put(HazelcastSingleton.getNodeId(), Tungsten.session_id);
        Tungsten.ctx = Context.createContext(Tungsten.session_id);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutDown();
            }
        }));
        final String isShutDown = System.getProperty("com.datasphere.config.shutdown");
        if (isShutDown != null && isShutDown.equals("true")) {
            if (Tungsten.currUserMetaInfo.getName().equalsIgnoreCase("admin")) {
                final String allcluster = System.getProperty("com.datasphere.config.shutallcluster");
                String actionstr;
                if (allcluster != null && allcluster.equalsIgnoreCase("true")) {
                    actionstr = "ALL";
                }
                else {
                    final String ipAddress = System.getProperty("com.datasphere.config.interface");
                    final String port = System.getProperty("com.datasphere.config.port");
                    if (ipAddress == null || port == null) {
                        printf("IP Address and Port must be specified for server shutdown\n");
                        System.exit(Tungsten.NUM_OF_ERRORS.get());
                    }
                    actionstr = "/" + ipAddress + ":" + port;
                }
                printf("Initiated shutdown\n");
                try {
                    Tungsten.ctx.executeShutdown(actionstr);
                }
                catch (Throwable e) {
                    printf("ERROR:" + e.getLocalizedMessage() + "\n");
                }
            }
            else {
                printf("Only Admin user can initiate shutdown\n");
                System.exit(Tungsten.NUM_OF_ERRORS.get());
            }
            System.exit(Tungsten.NUM_OF_ERRORS.get());
            return;
        }
        process("use " + Tungsten.currUserMetaInfo.getDefaultNamespace() + ";");
        argsController(args);
        final Tungsten console = new Tungsten();
        final String clFormat = System.getProperty("com.datasphere.config.prompt-format");
        if (clFormat != null && !clFormat.isEmpty()) {
            Tungsten.commandLineFormat = clFormat;
        }
        console.offerHelp();
        final Deque<String> history = new ArrayDeque<String>();
        ConsoleReader.enableHistory();
        while (!Tungsten.quit) {
            try {
                String cmd = ConsoleReader.readLine(console.getPrompt());
                if (cmd == null) {
                    break;
                }
                if (cmd.isEmpty()) {
                    continue;
                }
                if (cmd.toUpperCase().startsWith("HISTORY")) {
                    final String result = handleHistory(cmd, history);
                    if (result == null || result.isEmpty()) {
                        continue;
                    }
                    cmd = result;
                    System.out.println(cmd);
                }
                history.addLast(cmd);
                String firstWord = cmd.trim().toUpperCase().split(" ")[0];
                if (firstWord.endsWith(";")) {
                    firstWord = firstWord.substring(0, firstWord.length() - 1);
                }
                final String s2 = firstWord;
                switch (s2) {
                    case "BATCHMODE": {
                        console.setBatchmode(cmd);
                        continue;
                    }
                    case "HELP": {
                        console.help(cmd);
                        continue;
                    }
                    case "GO": {
                        console.executeBatch();
                        continue;
                    }
                    case "SPOOL": {
                        spool(cmd);
                        continue;
                    }
                    case "BACKUP": {
                        backup(cmd);
                        continue;
                    }
                    case "MDUMP": {
                        if (!checkGlobalAdminRoleForUser(Tungsten.ctx.getAuthToken())) {
                            printf("Insufficient privileges for Operation");
                            continue;
                        }
                        final Map map = Tungsten.clientOps.dumpMaps(Tungsten.ctx.getAuthToken());
                        for (final Object o : map.keySet()) {
                            final Object val = map.get(o);
                            String strVal;
                            if (val instanceof MetaInfo.MetaObject) {
                                strVal = ((MetaInfo.MetaObject)val).JSONifyString();
                            }
                            else {
                                strVal = val.toString();
                            }
                            multiPrint(o + ": " + strVal + "\n");
                        }
                        continue;
                    }
                    case "WACACHE": {
                        dumpCaches(cmd);
                        continue;
                    }
                    case "SLEEP": {
                        final String[] s = cmd.split("\\s+");
                        final int t = Integer.parseInt(s[1]);
                        Thread.sleep(t);
                        continue;
                    }
                    case "CDUMP": {
                        if (!checkGlobalAdminRoleForUser(Tungsten.ctx.getAuthToken())) {
                            printf("Insufficient privileges for Operation");
                            continue;
                        }
                        HDLoader.get().listAllBundles();
                        continue;
                    }
                    case "CID": {
                        if (!checkGlobalAdminRoleForUser(Tungsten.ctx.getAuthToken())) {
                            printf("Insufficient privileges for Operation");
                            continue;
                        }
                        HDLoader.get().listClassAndId();
                        continue;
                    }
                    case "MGET": {
                        final String[] ar = cmd.trim().split(" ");
                        if (ar.length <= 1) {
                            continue;
                        }
                        assert ar.length == 4;
                        printf("MetaObject " + ar[1] + " = " + Tungsten.clientOps.getMetaObjectByName(EntityType.valueOf(ar[0]), ar[1], ar[2], Integer.getInteger(ar[3]), Tungsten.session_id));
                        continue;
                    }
                    case "TEST": {
                        testCmd(cmd);
                        continue;
                    }
                    case "USAGE": {
                        Usage.execute(Tungsten.ctx, cmd);
                        continue;
                    }
                    default: {
                        console.executeCommand(cmd);
                        continue;
                    }
                }
            }
            catch (Exception e2) {
                Tungsten.logger.error((Object)"Problem executing statement", (Throwable)e2);
            }
        }
        printf("\nQuit.");
        closeWebSocket();
        System.exit(Tungsten.NUM_OF_ERRORS.get());
    }
    
    private static void spool(String cmd) {
        cmd = cmd.trim();
        if (cmd.endsWith(";")) {
            cmd = cmd.substring(0, cmd.length() - 1);
        }
        final String[] parts = cmd.trim().split(" ");
        if (parts.length > 4 || parts.length < 2) {
            printf(Tungsten.SPOOL_ERROR_MSG);
            return;
        }
        if (parts[1].equalsIgnoreCase("ON")) {
            String fileName = Tungsten.HD_LOG;
            boolean spoolConditionsMet = false;
            if (parts.length == 2) {
                spoolConditionsMet = true;
            }
            if (parts.length == 4 && parts[2].equalsIgnoreCase("TO")) {
                fileName = parts[3];
                spoolConditionsMet = true;
            }
            if (spoolConditionsMet) {
                SpoolFile.spoolMode.set(true);
                SpoolFile.fileName = fileName;
                try {
                    SpoolFile.writer = new PrintWriter(SpoolFile.fileName, "UTF-8");
                }
                catch (FileNotFoundException | UnsupportedEncodingException ex2) {
                    Tungsten.logger.error((Object)ex2.getMessage());
                }
            }
            else {
                printf(Tungsten.SPOOL_ERROR_MSG);
            }
        }
        else if (parts[1].equalsIgnoreCase("OFF")) {
            SpoolFile.spoolMode.set(false);
            if (SpoolFile.writer != null) {
                SpoolFile.writer.close();
                SpoolFile.writer = null;
            }
            SpoolFile.fileName = null;
        }
        else {
            printf(Tungsten.SPOOL_ERROR_MSG);
        }
    }
    
    private static String handleHistory(String cmd, final Deque<String> history) {
        cmd = cmd.trim();
        if (cmd.endsWith(";")) {
            cmd = cmd.substring(0, cmd.length() - 1);
        }
        final String[] parts = cmd.trim().toUpperCase().split(" ");
        if (parts.length >= 2 && parts[1].equalsIgnoreCase("clear")) {
            history.clear();
            printf("Cleared history\n");
            return null;
        }
        if (parts.length >= 2 && NumberUtils.isNumber(parts[1]) && !history.isEmpty()) {
            Integer ii;
            try {
                ii = Integer.parseInt(parts[1]);
            }
            catch (NumberFormatException e) {
                Tungsten.logger.error((Object)("Bad number format, must be integer: " + parts[1]));
                return null;
            }
            ii %= history.size();
            if (ii <= 0) {
                ii += history.size();
            }
            final Iterator<String> it = history.iterator();
            String hist_cmd = "";
            int jj = 1;
            while (it.hasNext()) {
                if (jj == ii) {
                    hist_cmd = it.next();
                    break;
                }
                it.next();
                ++jj;
            }
            return hist_cmd;
        }
        final Iterator<String> it2 = history.iterator();
        int num = 1;
        while (it2.hasNext()) {
            System.out.printf("%5d   %s\n", num++, it2.next());
            System.out.flush();
        }
        return null;
    }
    
    private void offerHelp() {
        printf("Type 'HELP' to get some assistance, or enter commands below\n");
    }
    
    private void executeCommand(String cmd) {
        cmd += "\n";
        if (cmd.equals(";") || cmd.isEmpty()) {
            return;
        }
        final StringBuilder strAcc = new StringBuilder();
        strAcc.append(cmd).append(" ");
        boolean isbalanced = isBalanced(strAcc.toString());
        if (cmd.trim().endsWith(";") && !this.isBatch && isbalanced) {
            process(strAcc.toString());
            return;
        }
        try {
            String line;
            while ((line = ConsoleReader.readLine("... ")) != null) {
                if (line.equals("GO") && this.isBatch) {
                    process(strAcc.toString());
                    this.executeBatch();
                    break;
                }
                if (line.trim().endsWith(";") && !this.isBatch) {
                    line += "\n";
                    strAcc.append(line);
                    isbalanced = isBalanced(strAcc.toString());
                    if (!this.isBatch && isbalanced) {
                        process(strAcc.toString());
                        break;
                    }
                    continue;
                }
                else if (line.trim().endsWith(";") && this.isBatch) {
                    strAcc.append(line).append(" ");
                }
                else {
                    line += "\n";
                    strAcc.append(line).append(" ");
                }
            }
        }
        catch (Exception e) {
            Tungsten.logger.error((Object)e);
        }
    }
    
    public static void process(final String text) {
        processWithContext(text, Tungsten.ctx);
    }
    
    public static void processWithContext(final String text, final Context ctx) {
        final AtomicBoolean isItLoadStatement = new AtomicBoolean(false);
        final long beg = System.currentTimeMillis();
        try {
            Compiler.compile(text, ctx, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    if (stmt instanceof LoadFileStmt) {
                        isItLoadStatement.set(true);
                    }
                    if (!isItLoadStatement.get()) {
                        Tungsten.printf("Processing - " + stmt.sourceText.replaceAll("%", "%%") + "\n");
                    }
                    try {
                        final Object result = compiler.compileStmt(stmt);
                        if (result instanceof MetaInfo.Query) {
                            Tungsten.query = (MetaInfo.Query)result;
                            if (Tungsten.query.isAdhocQuery()) {
                                try {
                                    Tungsten.isAdhocRunning.set(true);
                                    ctx.startAdHocQuery(Tungsten.query);
                                    ConsoleReader.readLine();
                                }
                                catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                finally {
                                    ctx.stopAndDropAdHocQuery(Tungsten.query);
                                    Tungsten.isAdhocRunning.set(false);
                                }
                            }
                        }
                        else if (result instanceof DumpStmt) {
                            final DumpStmt dmpstmt = (DumpStmt)result;
                            Tungsten.isQueryDumpOn.set(true);
                            try {
                                ConsoleReader.readLine();
                            }
                            catch (IOException e2) {
                                throw new Warning(e2);
                            }
                            finally {
                                Tungsten.isQueryDumpOn.set(false);
                                ctx.changeCQInputOutput(dmpstmt.getCqname(), dmpstmt.getDumpmode(), 0, true, Tungsten.session_id);
                            }
                        }
                    }
                    catch (Warning e3) {
                        Tungsten.printf("-> FAILURE \n");
                        Tungsten.NUM_OF_ERRORS.incrementAndGet();
                        String msg = e3.getLocalizedMessage();
                        if (e3.getCause() instanceof Warning) {
                            msg = e3.getCause().getLocalizedMessage();
                        }
                        Tungsten.printf(msg + "\n");
                        if (Tungsten.logger.isInfoEnabled()) {
                            Tungsten.logger.info((Object)e3, (Throwable)e3);
                        }
                        return;
                    }
                    if (!(stmt instanceof CreatePropertySetStmt) && !(stmt instanceof CreateUserStmt) && !(stmt instanceof ConnectStmt) && !(stmt instanceof CreatePropertyVariableStmt)) {
                        if (stmt.sourceText == null || stmt.sourceText.isEmpty()) {
                            storeCommand(text, null);
                        }
                        else {
                            storeCommand(stmt.getRedactedSourceText(), null);
                        }
                    }
                    if (!isItLoadStatement.get()) {
                        Tungsten.printf("-> SUCCESS \n");
                    }
                    if (stmt.returnText != null && !stmt.returnText.isEmpty()) {
                        for (final Object obj : stmt.returnText) {
                            if (obj instanceof String && !((String)obj).isEmpty()) {
                                Tungsten.printf((String)obj);
                            }
                        }
                    }
                }
            });
        }
        catch (Throwable e) {
            printf("-> FAILURE \n");
            Tungsten.NUM_OF_ERRORS.incrementAndGet();
            if (e instanceof NullPointerException) {
                printf("Internal error:" + e + "\n");
                for (final StackTraceElement s : e.getStackTrace()) {
                    printf(s + "\n");
                }
            }
            else if (e.getCause() instanceof ClassNotFoundException) {
                final String msg = "Class not found in the lib path : " + e.getLocalizedMessage();
                printf(msg + "\n");
            }
            else if (e.getLocalizedMessage().contains("com.datasphere.exception.InvalidTokenException")) {
                Tungsten.logger.error((Object)(e.getLocalizedMessage() + " .Please login again."));
                System.exit(-1);
            }
            else if (e.getCause() instanceof Warning) {
                final String msg = e.getCause().getLocalizedMessage();
                printf(msg + "\n");
            }
            else {
                final String msg = e.getLocalizedMessage();
                printf(msg + "\n");
            }
            if (Tungsten.logger.isInfoEnabled()) {
                Tungsten.logger.info((Object)e, e);
            }
        }
        final long end = System.currentTimeMillis();
        if (!isItLoadStatement.get()) {
            printf("Elapsed time: " + (end - beg) + " ms\n");
        }
    }
    
    public static void setCommandLog(final boolean commandLog) {
        Tungsten.commandLoggingEnabled = commandLog;
    }
    
    private static void storeCommand(String text, final String methodName) {
        if (Tungsten.commandLoggingEnabled) {
            try {
                String json = "{\"class\":\"com.datasphere.runtime.QueryValidator\", \"method\":\"storeCommand\",\"params\":[\"#sessionid#\", \"#userid#\", \"#command#\"],\"callbackIndex\":2}";
                if (Tungsten.tungsteClient == null || !Tungsten.tungsteClient.isConnected()) {
                    final String serverSocketUrl = "ws://" + getServerIp() + ":9080/rmiws/";
                    if (Tungsten.logger.isInfoEnabled()) {
                        Tungsten.logger.info((Object)("serverSocketUrl: " + serverSocketUrl));
                    }
                    (Tungsten.tungsteClient = new SimpleWebSocketClient(serverSocketUrl)).start();
                }
                json = json.replace("#sessionid#", Tungsten.session_id.getUUIDString());
                text = text.replaceAll("(\\r|\\n)", "");
                text = StringEscapeUtils.escapeJava(text);
                json = json.replace("#command#", text);
                json = json.replace("#userid#", Tungsten.currUserMetaInfo.getName());
                if (methodName != null) {
                    json = json.replace("storeCommand", methodName);
                }
                if (Tungsten.logger.isDebugEnabled()) {
                    Tungsten.logger.debug((Object)("sending text :" + json));
                }
                Tungsten.tungsteClient.sendMessages(json);
            }
            catch (Exception ex) {
                Tungsten.logger.error((Object)"error logging command:", (Throwable)ex);
            }
        }
    }
    
    private static void closeWebSocket() {
        Tungsten.tungsteClient.close();
    }
    
    private static String getServerIp() {
        final Set<Member> srvs = DistributedExecutionManager.getFirstServer(HazelcastSingleton.get());
        if (srvs.isEmpty()) {
            throw new RuntimeException("No available Node to get Monitor data");
        }
        final HazelcastMemberAddressProvider hzMemberAddressProvider = DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), () -> HazelcastSingleton.getHzmemberAddressProvider());
        if (hzMemberAddressProvider.getPublicAddress() == null) {
            return hzMemberAddressProvider.getBindAddress().getHostName();
        }
        if (hzMemberAddressProvider.getPublicAddress().equals(hzMemberAddressProvider.getBindAddress())) {
            return hzMemberAddressProvider.getBindAddress().getHostName();
        }
        if (hzMemberAddressProvider.getBindAddress().getHostName().equals(HazelcastSingleton.getBindingInterface())) {
            return hzMemberAddressProvider.getBindAddress().getHostName();
        }
        return hzMemberAddressProvider.getPublicAddress().getHostName();
    }
    
    public static void process(final String text, final Context mockContext) {
        final long beg = System.currentTimeMillis();
        try {
            Compiler.compile(text, mockContext, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    Tungsten.printf("Processing - " + stmt.sourceText.replaceAll("%", "%%") + "\n");
                    final Object result = compiler.compileStmt(stmt);
                    if (result instanceof MetaInfo.Query) {
                        final MetaInfo.Query query = (MetaInfo.Query)result;
                        if (query.isAdhocQuery()) {
                            try {
                                Tungsten.isAdhocRunning.set(true);
                                mockContext.startAdHocQuery(query);
                                ConsoleReader.readLine();
                            }
                            catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            finally {
                                mockContext.stopAndDropAdHocQuery(query);
                                Tungsten.isAdhocRunning.set(false);
                            }
                        }
                    }
                    Tungsten.printf("-> SUCCESS \n");
                }
            });
        }
        catch (Throwable e) {
            printf("-> FAILURE \n");
            if (e instanceof NullPointerException) {
                printf("Internal error:" + e + "\n");
                for (final StackTraceElement s : e.getStackTrace()) {
                    printf(s + "\n");
                }
            }
            else if (e.getCause() instanceof ClassNotFoundException) {
                final String msg = "Class not found in the lib path: " + e.getLocalizedMessage();
                printf(msg + "\n");
            }
            else {
                final String msg = e.getLocalizedMessage();
                printf(msg + "\n");
            }
            if (Tungsten.logger.isInfoEnabled()) {
                Tungsten.logger.info((Object)e, e);
            }
        }
        final long end = System.currentTimeMillis();
        printf("Elapsed time: " + (end - beg) + " ms\n");
    }
    
    private void executeBatch() {
        this.isBatch = false;
    }
    
    private void help(final String cmd) {
        final String[] ar = cmd.split(" ");
        if (ar.length == 1) {
            printf("Usage: \n");
            final Keywords[] values;
            final Keywords[] menuOps = values = Keywords.values();
            for (final Keywords menuOp : values) {
                System.out.printf("%9s  %s\n", menuOp.toString(), Tungsten.enumMap.get(menuOp));
            }
            System.out.printf("%9s  %s\n", "USAGE", "[Display usage information]\n           'USAGE' displays usage information for the entire installation\n           'USAGE <application>' displays usage information for a single application");
            System.out.flush();
            printf("        @  [Run BatchScript e.g. \"@{path}{file}\" ]\n");
        }
        else if (ar.length == 2) {
            String noSemicolon = ar[1].toUpperCase();
            if (noSemicolon.endsWith(";")) {
                noSemicolon = noSemicolon.substring(0, noSemicolon.length() - 1).toUpperCase();
            }
            final String s = noSemicolon;
            switch (s) {
                case "TYPE": {
                    printf("Example :\n");
                    printf("CREATE TYPE PosData(  \n merchantId String, \n dateTime java.util.Date, \n hourValue int,  \n amount double,  \n zip String );\n");
                    break;
                }
                case "SOURCE": {
                    printf("Example :\n");
                    printf("CREATE source CsvDataSource USING CSVReader (\n  directory:'../HDTests/data',\n  header:Yes,\n  wildcard:'posdata.csv',\n  coldelimiter:','\n) OUTPUT TO CsvStream;\n");
                    break;
                }
                case "STREAM": {
                    printf("Example :\n");
                    printf("CREATE STREAM PosDataStream OF PosData;\n");
                    break;
                }
                case "TARGET": {
                    printf("Example :\n");
                    printf("CREATE TARGET SendAlert USING AlertSender(\n        name: unusualActivity\n       ) INPUT FROM AlertStream;\n");
                    break;
                }
                case "NAMESPACE": {
                    printf("Example :\n");
                    printf("CREATE NAMESPACE PosApp;\n");
                    printf("USE PosApp; // To select namespace\n");
                    break;
                }
                case "WINDOW": {
                    printf("Example :\n");
                    printf("CREATE JUMPING WINDOW PosData5Minutes\nOVER PosDataStream KEEP WITHIN 5 MINUTE ON dateTime\nPARTITION BY merchantId;\n\n");
                    break;
                }
                case "CACHE": {
                    printf("Example :\n");
                    printf("CREATE CACHE HourlyAveLookup using CSVReader (\n         directory: '../HDTests/data',\n         wildcard: 'hourlyData.txt',\n         header: Yes,\n         coldelimiter: ','\n       ) QUERY (keytomap:'merchantId') OF MerchantHourlyAve;\n\n");
                    break;
                }
                case "CQ": {
                    printf("Example :\n");
                    printf("CREATE CQ CsvToPosData\nINSERT INTO PosDataStream\nSELECT data[3], stringToDate(data[6],'yyyyMMddHHmmss'),\n      castToInt(substr(data[6],8,10)), \n      castToDouble(data[9]), data[11]\nFROM CsvStream;\n");
                    break;
                }
                case "APPLICATION": {
                    printf("Example :\n");
                    printf("CREATE APPLICATION PosApp (\n         SOURCE CsvDataSource,\n         STREAM CsvStream,\n         CQ CsvToPosData);");
                    printf("start APPLICATION PosApp\n");
                    break;
                }
                case "HDSTORE": {
                    printf("Example :\n");
                    printf("CREATE HDSTORE MerchantActivity\nCONTEXT OF MerchantActivityContext\nEVENT TYPES (\n  MerchantTxRate KEY(merchantId)\n) PERSIST NONE USING ( ) ;\n");
                    break;
                }
                case "MONITOR":
                case "MON": {
                    CluiMonitorView.printUsage();
                    break;
                }
                case "SHOW": {
                    ShowCommandHelper.showUsage();
                    break;
                }
                case "REPORT": {
                    ReportCommandHelper.showUsage();
                    break;
                }
                default: {
                    printf("Incorrect object type\n");
                    break;
                }
            }
        }
    }
    
    private void setBatchmode(final String cmd) {
        final String[] arr = cmd.split(" ");
        if (arr.length <= 2) {
            return;
        }
        this.isBatch = arr[1].equalsIgnoreCase("true");
    }
    
    public static boolean isBalanced(final String expression) {
        final char QUOTE = '\"';
        final char SINGLEQUOTE = '\'';
        final Stack<Character> store = new Stack<Character>();
        final boolean failed = false;
        boolean incomment = false;
        for (int i = 0; i < expression.length(); ++i) {
            switch (expression.charAt(i)) {
                case '/': {
                    if (expression.charAt(i + 1) == '*' && !Tungsten.insinglequote && !Tungsten.indoublequote) {
                        incomment = true;
                        ++i;
                        break;
                    }
                    break;
                }
                case '*': {
                    if (expression.charAt(i + 1) == '/' && !Tungsten.insinglequote && !Tungsten.indoublequote) {
                        incomment = false;
                        ++i;
                        break;
                    }
                    break;
                }
                case '\"': {
                    if ((store.isEmpty() || store.peek() != '\"') && !incomment && !Tungsten.insinglequote) {
                        store.push('\"');
                        Tungsten.indoublequote = true;
                        break;
                    }
                    if (!store.isEmpty() && store.peek() == '\"' && !incomment && !Tungsten.insinglequote) {
                        store.pop();
                        Tungsten.indoublequote = false;
                        break;
                    }
                    break;
                }
                case '\'': {
                    if ((store.isEmpty() || store.peek() != '\'') && !incomment && !Tungsten.indoublequote) {
                        store.push('\'');
                        Tungsten.insinglequote = true;
                        break;
                    }
                    if (!store.isEmpty() && store.peek() == '\'' && !incomment && !Tungsten.indoublequote) {
                        store.pop();
                        Tungsten.insinglequote = false;
                        break;
                    }
                    break;
                }
            }
        }
        return store.isEmpty() && !failed && !incomment;
    }
    
    static void dumpCaches(String cmd) throws Exception {
        if (!cmd.endsWith(";")) {
            cmd = cmd.substring(0, cmd.length());
        }
        else {
            cmd = cmd.substring(0, cmd.length() - 1);
        }
        final String[] args = cmd.trim().split(" ");
        String cacheName = null;
        boolean detail = false;
        if (args.length >= 2) {
            cacheName = args[1];
            if (args.length == 3) {
                detail = "detail".equalsIgnoreCase(args[2]);
            }
        }
        final Set<String> cacheNames = PartitionManager.getAllCacheNames();
        if (cacheName != null && !cacheNames.contains(cacheName)) {
            printf("The specified WACACHE " + cacheName + " does not exist\n");
            return;
        }
        if (cacheName != null) {
            cacheNames.clear();
            cacheNames.add(cacheName);
        }
        if (cacheNames.isEmpty()) {
            printf("No WACACHEs found\n");
        }
        final CachingProvider provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
        final CacheManager manager = (CacheManager)provider.getCacheManager();
        final MutableConfiguration<Object, Object> configuration = new CacheConfiguration<Object, Object>();
        configuration.setStoreByValue(false);
        configuration.setStatisticsEnabled(false);
        printf("Analyzing the following caches: " + cacheNames + "\n");
        for (final String aCacheName : cacheNames) {
            final ICache<Object, Object> cache = (ICache<Object, Object>)(ICache)manager.createCache(aCacheName, configuration);
            manager.startCacheLite(aCacheName);
            final Set<ICache.CacheInfo> infos = cache.getCacheInfo();
            manager.destroyCache(aCacheName);
            int numServers = 0;
            for (final ICache.CacheInfo info : infos) {
                final MetaInfo.Server s = (MetaInfo.Server)Tungsten.clientOps.getMetaObjectByUUID(info.serverID, HSecurityManager.TOKEN);
                if (s != null) {
                    ++numServers;
                }
            }
            printf("WACACHE " + aCacheName + " on " + numServers + " servers.\n");
            int numPrimarys = 0;
            int numStale = 0;
            int primarySize = 0;
            int staleSize = 0;
            for (final ICache.CacheInfo info2 : infos) {
                final MetaInfo.Server s2 = (MetaInfo.Server)Tungsten.clientOps.getMetaObjectByUUID(info2.serverID, HSecurityManager.TOKEN);
                if (s2 != null) {
                    printf("  Server " + ((s2 != null) ? s2.name : info2.serverID) + " replication: " + ((info2.replicationFactor == 0) ? "all" : info2.replicationFactor) + ":");
                    printf(" Parts=" + info2.numParts + "/" + info2.totalSize);
                    printf(" Stale=" + info2.numStaleParts + "/" + info2.totalStaleSize);
                    printf(" Indices = " + info2.indices);
                    printf("\n");
                    printf(" \\-Replicas: ");
                    for (int i = 0; i < info2.replicaParts.length; ++i) {
                        printf("[" + i + "]=" + info2.replicaParts[i] + "/" + info2.replicaSizes[i] + " ");
                        if (i == 0) {
                            numPrimarys += info2.replicaParts[0];
                            primarySize += info2.replicaSizes[0];
                        }
                    }
                    printf("\n");
                    if (detail) {
                        printf("    Parts[num][replica] = " + info2.parts + "\n");
                    }
                    numStale += info2.numStaleParts;
                    staleSize += info2.totalStaleSize;
                }
            }
            printf("-->Total:");
            printf(" P=" + numPrimarys + "/" + primarySize);
            printf(" S=" + numStale + "/" + staleSize);
            printf("\n");
        }
    }
    
    public static void describe(String cmd) throws MetaDataRepositoryException {
        if (!cmd.endsWith(";")) {
            cmd = cmd.trim();
        }
        else {
            cmd = cmd.substring(0, cmd.length() - 1).trim();
        }
        final String[] parts = cmd.trim().split(" ");
        EntityType type = null;
        String name;
        if (parts.length == 3) {
            name = parts[2];
            String typeVal = parts[1].toUpperCase();
            if ("CLUSTER".equals(typeVal) || "ASIS".equals(typeVal)) {
                typeVal = EntityType.INITIALIZER.toString();
            }
            try {
                if (typeVal.equalsIgnoreCase("subscription")) {
                    typeVal = "TARGET";
                }
                if (typeVal.equalsIgnoreCase("namedquery") || typeVal.equalsIgnoreCase("query")) {
                    typeVal = "QUERY";
                }
                type = EntityType.valueOf(typeVal);
            }
            catch (IllegalArgumentException e) {
                multiPrint("Object type: " + typeVal + " is not a valid type\n");
                return;
            }
        }
        else {
            if (parts.length != 2) {
                multiPrint("Command: '" + cmd + "' is invalid. Please use 'describe [type] name'\n");
                return;
            }
            name = parts[1];
            if ("CLUSTER".equals(name.toUpperCase()) || "ASIS".equals(name.toUpperCase())) {
                type = EntityType.INITIALIZER;
            }
        }
        if (type == null || !type.equals(EntityType.INITIALIZER)) {
            final List<MetaInfo.MetaObject> results = new ArrayList<MetaInfo.MetaObject>();
            for (final EntityType aType : EntityType.values()) {
                if (type == null || (type != null && aType.equals(type))) {
                    MetaInfo.MetaObject obj = null;
                    if (aType.equals(EntityType.NAMESPACE) || aType.equals(EntityType.SERVER) || aType.equals(EntityType.PROPERTYTEMPLATE)) {
                        if (aType.equals(EntityType.SERVER)) {
                            obj = MetadataRepository.getINSTANCE().getServer(name, Tungsten.session_id);
                        }
                        else if (aType.equals(EntityType.PROPERTYTEMPLATE)) {
                            final Set<MetaInfo.PropertyTemplateInfo> ptSet = (Set<MetaInfo.PropertyTemplateInfo>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYTEMPLATE, Tungsten.session_id);
                            for (final MetaInfo.PropertyTemplateInfo pti : ptSet) {
                                if (pti.name.equals(name)) {
                                    results.add(pti);
                                }
                            }
                        }
                        else {
                            obj = Tungsten.clientOps.getMetaObjectByName(aType, "Global", name, null, Tungsten.session_id);
                        }
                    }
                    else if (name.indexOf(46) != -1) {
                        final String ns = name.split("\\.")[0];
                        final String objectname = name.split("\\.")[1];
                        obj = Tungsten.clientOps.getMetaObjectByName(aType, ns, objectname, null, Tungsten.session_id);
                    }
                    else if (aType.isGlobal()) {
                        obj = Tungsten.clientOps.getMetaObjectByName(aType, "Global", name, null, Tungsten.session_id);
                    }
                    else {
                        obj = Tungsten.clientOps.getMetaObjectByName(aType, Tungsten.ctx.getCurNamespace().getName(), name, null, Tungsten.session_id);
                    }
                    if (obj != null) {
                        results.add(obj);
                    }
                }
            }
            if (results.size() == 0) {
                multiPrint("No " + ((type == null) ? "objects" : type.name().toLowerCase()) + " " + name + " found.\n");
            }
            else {
                if (results.size() > 1) {
                    multiPrint("Found " + results.size() + " objects\n");
                }
                for (final MetaInfo.MetaObject obj2 : results) {
                    if (!obj2.getMetaInfoStatus().isDropped()) {
                        multiPrint("\n" + obj2.describe(Tungsten.session_id) + "\n");
                        if (obj2.getType() != EntityType.SOURCE && obj2.getType() != EntityType.APPLICATION && obj2.getType() != EntityType.FLOW) {
                            continue;
                        }
                        final String s = describeRestartPosition(obj2.getUuid());
                        multiPrint(s + "\n");
                    }
                }
            }
            return;
        }
        if (!checkGlobalAdminRoleForUser(Tungsten.ctx.getAuthToken())) {
            printf("Only users with Global admin role can look for cluster information\n");
            return;
        }
        final IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
        final String clusterName = HazelcastSingleton.getClusterName();
        final MetaInfo.Initializer initializer = (MetaInfo.Initializer)startUpMap.get((Object)clusterName);
        multiPrint(initializer.describe(Tungsten.session_id) + "\n");
    }
    
    public static String describeRestartPosition(final UUID componentUuid) {
        final String s = getCheckpointSummary(componentUuid);
        return (s == null || s.isEmpty()) ? "CHECKPOINT ( NONE )" : ("CHECKPOINT (" + s + "\n)");
    }
    
    public static String getCheckpointSummary(final UUID componentUuid) {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
        final GetRestartPosition action = new GetRestartPosition(componentUuid);
        try {
            final Collection<String> manyResults = DistributedExecutionManager.exec(hz, (Callable<String>)action, srvs);
            if (manyResults.size() < 1) {
                return null;
            }
            return manyResults.iterator().next();
        }
        catch (Exception e) {
            Tungsten.logger.error((Object)e);
            return null;
        }
    }
    
    static void listSessions(final String cmd) throws MetaDataRepositoryException {
        if (!checkGlobalAdminRoleForUser(Tungsten.ctx.getAuthToken())) {
            printf("Current User is not authorized to perform \"list sessions\" command\n");
            return;
        }
        final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        for (final AuthToken sessionToken : authTokens.keySet()) {
            final SessionInfo session = (SessionInfo)authTokens.get((Object)sessionToken);
            final String isCurrentSession = Tungsten.ctx.getAuthToken().equals((Object)sessionToken) ? "*" : " ";
            final Date date = new Date(session.loginTime);
            final String month = DateFormatSymbols.getInstance().getShortMonths()[date.getMonth()];
            final String time = new SimpleDateFormat("HH:mm").format(date);
            System.out.printf("%s %-15s %-3s %2d %s %18s %n", isCurrentSession, session.userId, month, date.getDate(), time, session.type);
        }
    }
    
    private static boolean checkGlobalAdminRoleForUser(final AuthToken token) throws MetaDataRepositoryException {
        final String userName = HSecurityManager.getAutheticatedUserName(token);
        final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", userName, null, Tungsten.ctx.getAuthToken());
        if (Tungsten.currUserMetaInfo == null || !(mo instanceof MetaInfo.User)) {
            printf("Could not find expected current user " + userName + "\n");
            return false;
        }
        final MetaInfo.User currentUser = (MetaInfo.User)mo;
        return currentUser.hasGlobalAdminRole();
    }
    
    public static void list(String cmd) throws MetaDataRepositoryException {
        if (!cmd.endsWith(";")) {
            cmd = cmd.trim();
        }
        else {
            cmd = cmd.substring(0, cmd.length() - 1).trim();
        }
        final String[] ar = cmd.trim().split(" ");
        if (ar.length != 2) {
            printf("LIST command requires exactly one parameter\n");
            return;
        }
        if (ar[1].equalsIgnoreCase("OBJECTS")) {
            final String[] array;
            final String[] possibleTypes = array = new String[] { "TYPES", "STREAMS", "SOURCES", "TARGETS", "NAMESPACES", "WINDOWS", "EVENTTABLES", "CACHES", "CQS", "APPLICATIONS", "FLOWS", "PROPERTYSETS", "PROPERTYVARIABLES", "HDSTORES", "PROPERTYTEMPLATES", "SUBSCRIPTIONS", "SERVERS", "DGS", "USERS", "ROLES", "NAMEDQUERIES", "PAGES", "DASHBOARDS", "QUERYVISUALIZATIONS" };
            for (final String type : array) {
                printType(type, null);
            }
        }
        else {
            printType(ar[1], cmd);
        }
    }
    
    private static void printType(final String type, final String cmd) throws MetaDataRepositoryException {
        final String PluralToSing = type.substring(0, type.length() - 1).toUpperCase();
        final String upperCase = type.toUpperCase();
        switch (upperCase) {
            case "TYPES": {
                printPretty(Utility.removeInternalApplications((Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.TYPE, Tungsten.session_id)), PluralToSing);
                break;
            }
            case "STREAMS": {
                printPretty(Utility.removeInternalApplications((Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.STREAM, Tungsten.session_id)), PluralToSing);
                break;
            }
            case "SOURCES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.SOURCE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "TARGETS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.TARGET, Tungsten.session_id), PluralToSing);
                break;
            }
            case "NAMESPACES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.NAMESPACE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "WINDOWS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.WINDOW, Tungsten.session_id), PluralToSing);
                break;
            }
            case "EVENTTABLES":
            case "CACHES": {
                final Set<MetaInfo.MetaObject> result = (Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.CACHE, Tungsten.session_id);
                final Iterator<MetaInfo.MetaObject> iterator = result.iterator();
                while (iterator.hasNext()) {
                    final MetaInfo.Cache metaObject = (MetaInfo.Cache)iterator.next();
                    if (type.toUpperCase().equalsIgnoreCase("EVENTTABLES") && !metaObject.isEventTable()) {
                        iterator.remove();
                    }
                    if (type.toUpperCase().equalsIgnoreCase("CACHES") && metaObject.isEventTable()) {
                        iterator.remove();
                    }
                }
                printPretty(result, PluralToSing);
                break;
            }
            case "CQS": {
                printPretty(Utility.removeInternalApplications((Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.CQ, Tungsten.session_id)), PluralToSing);
                break;
            }
            case "APPLICATIONS": {
                printPretty(Utility.removeInternalApplications((Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.APPLICATION, Tungsten.session_id)), PluralToSing);
                break;
            }
            case "FLOWS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.FLOW, Tungsten.session_id), PluralToSing);
                break;
            }
            case "PROPERTYSETS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYSET, Tungsten.session_id), PluralToSing);
                break;
            }
            case "PROPERTYVARIABLES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYVARIABLE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "HDSTORES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.HDSTORE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "PROPERTYTEMPLATES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYTEMPLATE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "PROPERTYSET": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYSET, Tungsten.session_id), PluralToSing);
                break;
            }
            case "PROPERTYVARIABLE": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PROPERTYVARIABLE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "SUBSCRIPTIONS": {
                final Set temp = new HashSet();
                final Set<?> set = Tungsten.clientOps.getByEntityType(EntityType.TARGET, Tungsten.session_id);
                if (set != null) {
                    for (final Object obj : set) {
                        if (((MetaInfo.Target)obj).isSubscription()) {
                            temp.add(obj);
                        }
                    }
                }
                printPretty(temp, PluralToSing);
                break;
            }
            case "SERVERS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.SERVER, Tungsten.session_id), PluralToSing);
                break;
            }
            case "DEPLOYMENTGROUPS":
            case "DGS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.DG, Tungsten.session_id), PluralToSing);
                break;
            }
            case "USERS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.USER, Tungsten.session_id), PluralToSing);
                break;
            }
            case "ROLES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.ROLE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "NAMEDQUERIES": {
                printPretty(Utility.removeInternalApplications((Set<MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.QUERY, Tungsten.session_id)), "NAMEDQUERY");
                break;
            }
            case "WACACHES": {
                final Set<String> cacheNames = PartitionManager.getAllCacheNames();
                if (cacheNames != null) {
                    int i = 1;
                    for (final String cacheName : cacheNames) {
                        printf("WACACHE " + i++ + " =>  " + cacheName + "\n");
                    }
                    break;
                }
                break;
            }
            case "PAGES": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.PAGE, Tungsten.session_id), PluralToSing);
                break;
            }
            case "DASHBOARDS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.DASHBOARD, Tungsten.session_id), PluralToSing);
                break;
            }
            case "QUERYVISUALIZATIONS": {
                printPretty((Set<? extends MetaInfo.MetaObject>)Tungsten.clientOps.getByEntityType(EntityType.QUERYVISUALIZATION, Tungsten.session_id), PluralToSing);
                break;
            }
            case "SESSIONS": {
                listSessions(cmd);
                break;
            }
            default: {
                multiPrint("List command is followed by an object type in plural form. e.g: 'List namespaces'\n");
                break;
            }
        }
    }
    
    static void testCmd(String cmd) throws Exception {
        if (cmd.trim().endsWith(";")) {
            cmd = cmd.trim();
            cmd = cmd.substring(0, cmd.length() - 1);
        }
        final String[] tu = cmd.split(" ");
        if (tu.length > 1 && tu[1].equalsIgnoreCase("compatibility")) {
            storeCommand(cmd, "testCompatibility");
        }
        else {
            printf("USAGE : TEST COMPATIBILITY <optional: json file location on server>\n");
        }
    }
    
    static void printPretty(final Set<? extends MetaInfo.MetaObject> l, final String type) throws MetaDataRepositoryException {
        if (l == null) {
            multiPrint("No " + type + " found\n");
        }
        else {
            int i = 1;
            for (final MetaInfo.MetaObject object : l) {
                if (object instanceof MetaInfo.Namespace) {
                    multiPrint(type + " " + i++ + " =>  " + object.name + "\n");
                }
                else if (object instanceof MetaInfo.User) {
                    multiPrint(type + " " + i++ + " =>  " + object.name + "\n");
                }
                else if (object instanceof MetaInfo.Role) {
                    multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + "\n");
                }
                else if (object instanceof MetaInfo.Server) {
                    final List<String> dgroups = new ArrayList<String>();
                    for (final UUID sId : ((MetaInfo.Server)object).deploymentGroupsIDs) {
                        final MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)Tungsten.clientOps.getMetaObjectByUUID(sId, Tungsten.session_id);
                        if (dg != null) {
                            dgroups.add(dg.name);
                        }
                        else {
                            dgroups.add(sId.getUUIDString());
                        }
                    }
                    multiPrint((((MetaInfo.Server)object).isAgent ? "AGENT" : "SERVER") + " " + i++ + " =>  " + object.name + " [" + object.uuid + "] in " + dgroups + "\n");
                }
                else if (object instanceof MetaInfo.DeploymentGroup) {
                    final List<String> servers = new ArrayList<String>();
                    for (final UUID sId : ((MetaInfo.DeploymentGroup)object).groupMembers.keySet()) {
                        final MetaInfo.Server s = (MetaInfo.Server)Tungsten.clientOps.getMetaObjectByUUID(sId, Tungsten.session_id);
                        if (s != null) {
                            servers.add(s.name);
                        }
                        else {
                            servers.add(sId.getUUIDString());
                        }
                    }
                    final List<String> configuredServers = ((MetaInfo.DeploymentGroup)object).configuredMembers;
                    multiPrint(type + " " + i++ + " =>  " + object.name + " has actual servers " + servers + " and configured servers " + configuredServers + " with mininum required servers " + ((MetaInfo.DeploymentGroup)object).getMinimumRequiredServers() + " and limit applications to " + ((MetaInfo.DeploymentGroup)object).getMaxApps() + "\n");
                }
                else if (object instanceof MetaInfo.PropertyTemplateInfo) {
                    final MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)object;
                    if (pti.getAdapterVersion() != null && !pti.getAdapterVersion().equals("0.0.0")) {
                        multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + " VERSION " + pti.adapterVersion + "\n");
                    }
                    else {
                        multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + "\n");
                    }
                }
                else {
                    multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + "\n");
                }
            }
        }
    }
    
    static void backup(String cmd) {
        if (cmd.endsWith(";")) {
            cmd = cmd.substring(0, cmd.length() - 1);
        }
        final String[] ar = cmd.trim().split(" ");
        final String upperCase = ar[1].toUpperCase();
        switch (upperCase) {
            case "LIST": {
                final File backupsDir = new File("bin/DerbyBackups");
                if (!backupsDir.exists()) {
                    System.out.println("Backup dir not found: " + backupsDir.getAbsolutePath());
                    break;
                }
                final File[] potentialBackups = backupsDir.listFiles();
                int backupCount = 0;
                for (int i = 0; i < potentialBackups.length; ++i) {
                    final File pbu = potentialBackups[i];
                    if (pbu.isDirectory() && pbu.getName().matches("\\d\\d\\d\\d-\\d\\d-\\d\\d_\\d\\d-\\d\\d-\\d\\d")) {
                        System.out.println(i + ". " + pbu.getName());
                        ++backupCount;
                    }
                }
                if (backupCount == 0) {
                    System.out.println("No backups found in " + backupsDir.getAbsolutePath());
                    break;
                }
                break;
            }
            case "NOW": {
                try {
                    final Process p = Runtime.getRuntime().exec("bin/backupDerby.sh");
                    final BufferedReader isr = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    final BufferedReader esr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    String iline;
                    String eline;
                    do {
                        iline = isr.readLine();
                        if (iline != null) {
                            System.out.println(iline);
                        }
                        eline = esr.readLine();
                        if (eline != null) {
                            System.out.println(eline);
                        }
                    } while (iline != null || eline != null);
                }
                catch (IOException e) {
                    Tungsten.logger.error((Object)e);
                }
                break;
            }
        }
    }
    
    private String getPrompt() {
        String prompt = Tungsten.commandLineFormat;
        if (prompt.contains("%A")) {
            final String ns = (Tungsten.ctx == null) ? "" : Tungsten.ctx.getCurNamespace().name;
            prompt = prompt.replace("%A", ns);
        }
        if (prompt.contains("%U")) {
            final String userName = (Tungsten.currUserMetaInfo != null) ? Tungsten.currUserMetaInfo.getName() : "??";
            prompt = prompt.replace("%U", userName);
        }
        if (prompt.contains("%C")) {
            final String cluster = HazelcastSingleton.get().getName();
            prompt = prompt.replace("%C", cluster);
        }
        return prompt;
    }
    
    public static HQueue getSessionQueue() {
        return Tungsten.sessionQueue;
    }
    
    public static HQueue.Listener getQueuelistener() {
        return Tungsten.queuelistener;
    }
    
    public static void listLogLevels() {
        final StringBuilder append = new StringBuilder().append("Main LOGGER: ");
        final Logger logger = Tungsten.logger;
        final StringBuilder append2 = append.append(Logger.getRootLogger().getName()).append(" ");
        final Logger logger2 = Tungsten.logger;
        multiPrint(append2.append(Logger.getRootLogger().getLevel()).append("\n { \n").toString());
        long counter = 0L;
        final String format = "\t%s %d %s  %s\t%s%n";
        final Enumeration<Logger> loggerEnumeratin = (Enumeration<Logger>)LogManager.getCurrentLoggers();
        while (loggerEnumeratin.hasMoreElements()) {
            final Logger XX = loggerEnumeratin.nextElement();
            multiPrint(String.format(format, "LOGGER", ++counter, "=>", XX.getName(), (XX.getLevel() == null) ? "Not Set" : XX.getLevel()));
        }
        multiPrint(" } \n");
    }
    
    static {
        Tungsten.logger = Logger.getLogger((Class)Tungsten.class);
        Tungsten.isAdhocRunning = new AtomicBoolean(false);
        Tungsten.isQueryDumpOn = new AtomicBoolean(false);
        Tungsten.session_id = null;
        Tungsten.currUserMetaInfo = null;
        Tungsten.userTimeZone = null;
        Tungsten.quit = false;
        Tungsten.currentFormat = PrintFormat.JSON;
        Tungsten.commandLineFormat = "W (%A) > ";
        Tungsten.ctx = null;
        Tungsten.HD_LOG = "WA.LOG";
        Tungsten.enumMap = new EnumMap<Keywords, String>(Keywords.class);
        Tungsten.SPOOL_ERROR_MSG = "Error on spool command.\n";
        Tungsten.query = null;
        Tungsten.comments = "Tungsten Property File";
        Tungsten.isShuttingDown = false;
        Tungsten.NUM_OF_ERRORS = new AtomicInteger(0);
        Tungsten.commandLoggingEnabled = true;
        Tungsten.tungsteClient = null;
        Tungsten.out = null;
        Tungsten.SIZE = 11024;
    }
    
    public enum PrintFormat
    {
        JSON, 
        ROW_FORMAT;
    }
    
    public enum Keywords
    {
        HELP, 
        PREV, 
        HISTORY, 
        GO, 
        CONNECT, 
        SET, 
        SHOW, 
        QUIT, 
        EXIT, 
        LIST, 
        DESCRIBE, 
        MONITOR;
    }
    
    static class SpoolFile
    {
        static AtomicBoolean spoolMode;
        static String fileName;
        static PrintWriter writer;
        
        static {
            SpoolFile.spoolMode = new AtomicBoolean(false);
            SpoolFile.fileName = null;
            SpoolFile.writer = null;
        }
    }
    
    private static class GetRestartPosition implements RemoteCall<String>
    {
        private static final long serialVersionUID = 8814117252476324531L;
        private final UUID componentUuid;
        
        public GetRestartPosition(final UUID componentUuid) {
            this.componentUuid = componentUuid;
        }
        
        @Override
        public String call() throws Exception {
            final Collection<AppCheckpointSummary> summaries = StatusDataStore.getInstance().getLowestAppCheckpointSummaries(this.componentUuid);
            if (summaries.isEmpty()) {
                return null;
            }
            final StringBuilder text = new StringBuilder();
            for (final AppCheckpointSummary summary : summaries) {
                text.append("\n   ").append(summary.sourceUri).append(" ");
                text.append("\n   SOURCE RESTART POSITION ").append(summary.atOrAfter ? "^" : "@");
                if (summary.lowSourcePositionText.contains("\n")) {
                    final String[] split;
                    final String[] lines = split = summary.lowSourcePositionText.split("\n");
                    for (final String line : split) {
                        text.append("\n      ").append(line);
                    }
                }
                else {
                    text.append("\n      ").append(summary.lowSourcePositionText);
                }
                final String highText = (summary.highSourcePositionText != null && !summary.highSourcePositionText.isEmpty()) ? summary.highSourcePositionText : summary.lowSourcePositionText;
                text.append("\n   SOURCE CURRENT POSITION ").append(summary.atOrAfter ? "^" : "@");
                if (highText.contains("\n")) {
                    final String[] split2;
                    final String[] lines2 = split2 = highText.split("\n");
                    for (final String line2 : split2) {
                        text.append("\n      ").append(line2);
                    }
                }
                else {
                    text.append("\n      ").append(highText);
                }
            }
            return text.toString();
        }
        
        private boolean compareLowAndHighSourcePosition(final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition) {
            if (lowSourcePosition instanceof PartitionedSourcePosition) {
                final PartitionedSourcePosition lsp = (PartitionedSourcePosition)lowSourcePosition;
                final Map<String, SourcePosition> lspMap = (Map<String, SourcePosition>)lsp.getSourcePositions();
                final PartitionedSourcePosition hsp = (PartitionedSourcePosition)highSourcePosition;
                final Map<String, SourcePosition> hspMap = (Map<String, SourcePosition>)hsp.getSourcePositions();
                for (final Map.Entry<String, SourcePosition> entry : lspMap.entrySet()) {
                    if (lsp.compareTo((SourcePosition)hspMap.get(entry.getKey()), (String)entry.getKey()) != 0) {
                        continue;
                    }
                    return false;
                }
                return true;
            }
            return lowSourcePosition.compareTo(highSourcePosition) != 0;
        }
    }
}
