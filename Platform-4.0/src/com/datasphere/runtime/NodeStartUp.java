package com.datasphere.runtime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.license.LicenseManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataDBDetails;
import com.datasphere.runtime.exceptions.ErrorCode;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;

public class NodeStartUp
{
    private static Logger logger;
    private boolean startUpFileExists;
    private MetaInfo.Initializer initializer;
    private String bindingInterface;
    public static final String startup_file_name = "startUp.properties";
    public static final String ConfigDirName = "conf";
    private boolean nodeStarted;
    private static String platformHome;
    private final String from_constant_string = "com.datasphere.platform.from";
    
    public NodeStartUp(final boolean runningWithDB) {
        this.startUpFileExists = false;
        this.initializer = null;
        this.bindingInterface = null;
        this.nodeStarted = false;
        this.startUp(runningWithDB);
    }
    
    public static String encrypt(final String password, final String clusterName) {
        String returnPass = null;
        try {
            final Random r = new Random(clusterName.hashCode());
            final byte[] salt = new byte[8];
            r.nextBytes(salt);
            returnPass = HSecurityManager.encrypt(password, salt);
        }
        catch (UnsupportedEncodingException | GeneralSecurityException ex2) {
            NodeStartUp.logger.error(ex2.getLocalizedMessage());
        }
        return returnPass;
    }
    
    public MetaInfo.Initializer getInitializer() {
        return this.initializer;
    }
    
    public void startUp(final boolean runningWithDB) {
        setPlatformHome();
        final Properties props = this.readPropertiesFile();
        if (this.startUpFileExists) {
            List<ErrorCode.Error> e = null;
            if (props != null) {
                e = checkFormat(props);
            }
            else {
                NodeStartUp.logger.error("Properties were loaded from file but props map was still null.");
            }
            if (e == null || e.isEmpty()) {
                this.startingUpWithStartUpFile(props, runningWithDB);
            }
            else {
                this.handleErrors(props, e);
                this.writePropertiesToFile(props);
                this.startingUpWithStartUpFile(props, runningWithDB);
            }
        }
        else {
            try {
                final Properties lprop = new Properties();
                final Map<String, String> clusterDetails = this.takeClusterDetails();
                lprop.putAll(clusterDetails);
                final Map<String, String> licenseKeyDetails = this.takeLicenseKeyInfo(lprop);
                clusterDetails.putAll(licenseKeyDetails);
                if (!clusterDetails.isEmpty()) {
                    final HazelcastInstance instance = HazelcastSingleton.get(clusterDetails.get(InitializerParams.Params.getWAClusterName()));
                    if (instance != null) {
                        this.bindingInterface = HazelcastSingleton.getBindingInterface();
                        final IMap<String, MetaInfo.Initializer> startUpMap = instance.getMap("#startUpMap");
                        if (NodeStartUp.logger.isDebugEnabled()) {
                            NodeStartUp.logger.debug(("begin start up map : " + startUpMap.entrySet()));
                        }
                        final String cName = clusterDetails.get(InitializerParams.Params.getWAClusterName());
                        startUpMap.lock(cName);
                        System.out.println("Starting Server on cluster : " + cName);
                        if (startUpMap.containsKey(cName)) {
                            this.noStartUpFileExistsAndObjectInMemory(cName, clusterDetails, runningWithDB, startUpMap);
                        }
                        else {
                            this.noStartUpFileExistsAndObjectNotInMemory(clusterDetails, runningWithDB, startUpMap);
                        }
                        startUpMap.unlock(cName);
                        if (NodeStartUp.logger.isInfoEnabled()) {
                            NodeStartUp.logger.info("Node started up(startUp File does not Exist)");
                        }
                        this.setNodeStarted(true);
                    }
                    else {
                        NodeStartUp.logger.error("Hazelcast Instance was null");
                    }
                }
                else {
                    NodeStartUp.logger.error("Cluster details were not entered correctly and failed 3 times, please restart the system");
                }
            }
            catch (IOException e3) {
                NodeStartUp.logger.error("Input reader failed!!");
            }
            catch (RuntimeException e2) {
                System.err.println(ErrorCode.Error.SERVERCANNOTSTART);
                System.err.println("Reason : " + e2.getMessage());
            }
        }
    }
    
    private void handleErrors(final Properties props, final List<ErrorCode.Error> e) {
        boolean justTookClusterDetails = false;
        boolean justTookDBDetails = false;
        for (final ErrorCode.Error ee : e) {
            if (ee.equals(ErrorCode.Error.CLUSTERNAME)) {
                System.err.println(ee);
                final Map<String, String> clusterDetails = this.takeClusterDetails();
                justTookClusterDetails = true;
                props.putAll(clusterDetails);
            }
            else if (ee.equals(ErrorCode.Error.CLUSTERPASSWORD) && !justTookClusterDetails) {
                System.err.println(ee);
                final String pass = this.takePassword((String)props.get(InitializerParams.Params.getWAClusterName()));
                props.put(InitializerParams.Params.getWAClusterPassword(), pass);
            }
            else if (ee.equals(ErrorCode.Error.INTERFACES)) {
                System.err.println(ee);
                final String ss = HazelcastSingleton.chooseInterface(HazelcastSingleton.getInterface());
                if (NodeStartUp.logger.isInfoEnabled()) {
                    NodeStartUp.logger.info(("New interface after it got erased from file : " + ss));
                }
                props.put(InitializerParams.Params.getInterfaces(), ss);
            }
            else if ((ee.equals(ErrorCode.Error.DATABASELOCATION) || ee.equals(ErrorCode.Error.DATABASENAME) || ee.equals(ErrorCode.Error.DATABASEUNAME) || ee.equals(ErrorCode.Error.DATABASEPASS)) && !justTookDBDetails) {
                System.err.println(ee);
                HazelcastSingleton.get(props.getProperty(InitializerParams.Params.getWAClusterName()));
                final MetaDataDBDetails dbChecking = new MetaDataDBDetails();
                try {
                    final Map<String, String> dbDetails = dbChecking.takeDBDetails();
                    props.putAll(dbDetails);
                    justTookDBDetails = true;
                }
                catch (IOException e2) {
                    NodeStartUp.logger.error(e2.getMessage());
                }
            }
            else {
                if (!ee.equals(ErrorCode.Error.NOLICENSEKEYSET) && !ee.equals(ErrorCode.Error.NOPRODUCTKEYSET) && !ee.equals(ErrorCode.Error.NOCOMPANYNAMESET)) {
                    continue;
                }
                System.err.println(ee);
                try {
                    final Map<String, String> licenseKeyDetails = this.takeLicenseKeyInfo(props);
                    props.putAll(licenseKeyDetails);
                }
                catch (IOException e3) {
                    NodeStartUp.logger.error(e3.getMessage());
                }
            }
        }
    }
    
    private static List<ErrorCode.Error> checkFormat(final Properties props) {
        final List<ErrorCode.Error> returnValue = new ArrayList<ErrorCode.Error>();
        String ss = null;
        ss = (String)props.get(InitializerParams.Params.getWAClusterName());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.CLUSTERNAME);
        }
        ss = (String)props.get(InitializerParams.Params.getWAClusterPassword());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.CLUSTERPASSWORD);
        }
        ss = (String)props.get(InitializerParams.Params.getInterfaces());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.INTERFACES);
        }
        ss = (String)props.get(InitializerParams.Params.getLicenceKey());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.NOLICENSEKEYSET);
        }
        ss = (String)props.get(InitializerParams.Params.getProductKey());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.NOPRODUCTKEYSET);
        }
        ss = (String)props.get(InitializerParams.Params.getCompanyName());
        if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
            returnValue.add(ErrorCode.Error.NOCOMPANYNAMESET);
        }
        final String persist = System.getProperty("com.datasphere.config.persist");
        if (NodeStartUp.logger.isDebugEnabled()) {
            NodeStartUp.logger.debug(("persist :" + persist));
        }
        if (persist != null && persist.equalsIgnoreCase("True")) {
            ss = (String)props.get(InitializerParams.Params.getMetaDataRepositoryLocation());
            if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
                returnValue.add(ErrorCode.Error.DATABASELOCATION);
            }
            ss = (String)props.get(InitializerParams.Params.getMetaDataRepositoryDBname());
            if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
                returnValue.add(ErrorCode.Error.DATABASENAME);
            }
            ss = (String)props.get(InitializerParams.Params.getMetaDataRepositoryUname());
            if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
                returnValue.add(ErrorCode.Error.DATABASEUNAME);
            }
            ss = (String)props.get(InitializerParams.Params.getMetaDataRepositoryPass());
            if (ss == null || ss.isEmpty() || ss.equalsIgnoreCase("null")) {
                returnValue.add(ErrorCode.Error.DATABASEPASS);
            }
        }
        return returnValue;
    }
    
    private Collection<String> parseInterfacesParam(final String interfaces) {
        final Set<String> ret = new HashSet<String>();
        for (final String s : interfaces.split(",")) {
            ret.add(s);
        }
        return ret;
    }
    
    private void startingUpWithStartUpFile(final Properties props, final boolean runningWithDB) {
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("Starting with file");
        }
        final String cName = (String)props.get(InitializerParams.Params.getWAClusterName());
        System.out.println("Starting Server on cluster : " + cName);
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("Will search for initializer object with name: " + cName + " in memory."));
        }
        final String iniParams = (String)props.get(InitializerParams.Params.getInterfaces());
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("Interfaces found in startup file : " + iniParams));
        }
        System.out.println("Interfaces found in startup file : " + this.parseInterfacesParam(iniParams));
        HazelcastInstance instance = null;
        try {
            instance = HazelcastSingleton.get(cName, iniParams);
        }
        catch (Exception e) {
            System.err.println(ErrorCode.Error.SERVERCANNOTSTART);
            System.err.println("Reason : " + e.getMessage());
        }
        this.bindingInterface = HazelcastSingleton.getBindingInterface();
        final boolean writeOutInterfaces = HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces;
        if (instance != null) {
            final IMap<String, MetaInfo.Initializer> startUpMap = instance.getMap("#startUpMap");
            if (NodeStartUp.logger.isInfoEnabled()) {
                NodeStartUp.logger.info(("begin start up map : " + startUpMap.entrySet()));
            }
            if (startUpMap.containsKey(cName)) {
                this.startUpFileExistsAndObjectInMemory(cName, props, startUpMap, runningWithDB, writeOutInterfaces);
            }
            else {
                this.startUpFileExistsAndObjectNotInMemory(cName, props, startUpMap, runningWithDB, writeOutInterfaces);
            }
            if (NodeStartUp.logger.isInfoEnabled()) {
                NodeStartUp.logger.info("Node started up(startUp File Exists)");
            }
            this.setNodeStarted(true);
        }
    }
    
    public static String getPlatformHome() {
        if (NodeStartUp.logger.isTraceEnabled()) {
            NodeStartUp.logger.trace("Getting platform home");
        }
        return NodeStartUp.platformHome;
    }
    
    public static void setPlatformHome() {
        final String home_dir = System.getProperty("com.datasphere.platform.home");
        if (home_dir != null && home_dir.contains(";")) {
            final String[] home_dir_list = home_dir.split(";");
            for (int i = 0; i < home_dir_list.length; ++i) {
                if (home_dir_list[i].contains("Platform")) {
                    NodeStartUp.platformHome = home_dir_list[i];
                    break;
                }
            }
        }
        else {
            NodeStartUp.platformHome = home_dir;
        }
        if (NodeStartUp.platformHome == null) {
            NodeStartUp.platformHome = ".";
        }
    }
    
    private Properties readPropertiesFile() {
        final String filePath = getPlatformHome().concat("/").concat("conf").concat("/").concat("startUp.properties");
        final File f = new File(filePath);
        final Properties props = new Properties();
        if (f.exists()) {
            try {
                props.load(new FileInputStream(f));
                this.startUpFileExists = true;
            }
            catch (IOException | NullPointerException ex2) {
                NodeStartUp.logger.error("Error while reading extant start-up file.", (Throwable)ex2);
            }
        }
        if (props != null && !props.isEmpty()) {
            final String propsClusterName = (String)props.get(InitializerParams.Params.getWAClusterName());
            if (propsClusterName != null) {
                final String argClusterName = System.getProperty("com.datasphere.config.clusterName");
                if (argClusterName != null && !argClusterName.isEmpty() && !propsClusterName.equals(argClusterName)) {
                    NodeStartUp.logger.error(("Cluster name " + argClusterName + " conflicts with " + propsClusterName + " specified in startUp.properties file. Please use '-c " + propsClusterName + "' or clear startUp.properties file."));
                    System.exit(1);
                }
            }
            String dbPassword = (String)props.get(InitializerParams.Params.getMetaDataRepositoryPass());
            if (dbPassword == null) {
                dbPassword = Server.metaDataDbProvider.askDBPassword("w@ct10n");
                props.put(InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword);
                final Properties props_copy = new Properties();
                props_copy.putAll(props);
                this.writePropertiesToFile(props_copy);
            }
            else {
                try {
                    if (ServerUpgradeUtility.isUpgrading && this.isVersionBefore321()) {
                        if (this.isCurrentVersionGreaterThanEqualTo321()) {
                            final Properties props_copy = new Properties();
                            props_copy.putAll(props);
                            this.writePropertiesToFile(props_copy);
                        }
                    }
                    else {
                        final String dbPassword_decrypted = MetaDataDBDetails.decryptDBPassword(dbPassword, "hdrepos");
                        props.put(InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword_decrypted);
                    }
                }
                catch (Exception e2) {
                    NodeStartUp.logger.error(("Metadata password decryption failed, hence shutting down Server: " + e2.getMessage() + ". In Server upgrade routine: " + ServerUpgradeUtility.isUpgrading));
                    System.exit(1);
                }
            }
        }
        return props;
    }
    
    private boolean isVersionBefore321() {
        final String from = System.getProperty("com.datasphere.platform.from");
        if (from == null || from.isEmpty()) {
            return false;
        }
        final ReleaseNumber from_release_number = ReleaseNumber.createReleaseNumber(from);
        final ReleaseNumber base_release_number = ReleaseNumber.createReleaseNumber(3, 2, 1);
        return from_release_number.lessThan(base_release_number);
    }
    
    private boolean isCurrentVersionGreaterThanEqualTo321() {
        final String current_version_string = Version.getPOMVersionString();
        final ReleaseNumber current_release_number = ReleaseNumber.createReleaseNumber(current_version_string);
        if (current_release_number.isDevVersion()) {
            return true;
        }
        final ReleaseNumber base_release_number = ReleaseNumber.createReleaseNumber(3, 2, 1);
        return current_release_number.greaterThanEqualTo(base_release_number);
    }
    
    private synchronized Properties writePropertiesToFile(final Properties p) {
        final String dbPassword = (String)p.get(InitializerParams.Params.getMetaDataRepositoryPass());
        String dbPassword_encrypted = null;
        if (dbPassword != null) {
            try {
                dbPassword_encrypted = MetaDataDBDetails.encryptDBPassword(dbPassword, "hdrepos");
            }
            catch (GeneralSecurityException e) {
                NodeStartUp.logger.error("Metadata password encryption failed, hence shutting down Server", (Throwable)e);
                System.exit(1);
            }
            catch (UnsupportedEncodingException e2) {
                NodeStartUp.logger.error("Metadata password encryption failed, hence shutting down Server", (Throwable)e2);
                System.exit(1);
            }
            p.put(InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword_encrypted);
        }
        final File propertyFile = new File(getPlatformHome().concat("/").concat("conf").concat("/").concat("startUp.properties"));
        if (propertyFile.exists()) {
            propertyFile.delete();
        }
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("Location : " + propertyFile.getAbsolutePath()));
        }
        try {
            final FileOutputStream fos = new FileOutputStream(propertyFile);
            p.store(fos, "Initial Setup");
            fos.close();
        }
        catch (IOException e3) {
            NodeStartUp.logger.error(e3.getMessage());
        }
        return p;
    }
    
    public void startUpFileExistsAndObjectInMemory(final String cName, final Properties tempProps, final IMap<String, MetaInfo.Initializer> startUpMap, final boolean runningWithDB, final boolean writeOutInterfaces) {
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("In-memory object available");
        }
        startUpMap.lock(cName);
        (this.initializer = (MetaInfo.Initializer)startUpMap.get(cName)).currentData();
        boolean dataInFileIsIncorrect = false;
        final Properties properties = new Properties();
        if (runningWithDB && (!(tempProps).get(InitializerParams.Params.getMetaDataRepositoryLocation()).equals(this.initializer.MetaDataRepositoryLocation) || !(tempProps).get(InitializerParams.Params.getMetaDataRepositoryDBname()).equals(this.initializer.MetaDataRepositoryDBname) || !(tempProps).get(InitializerParams.Params.getMetaDataRepositoryUname()).equals(this.initializer.MetaDataRepositoryUname) || !(tempProps).get(InitializerParams.Params.getMetaDataRepositoryPass()).equals(this.initializer.MetaDataRepositoryPass))) {
            dataInFileIsIncorrect = true;
            (properties).put(InitializerParams.Params.getMetaDataRepositoryLocation(), this.initializer.MetaDataRepositoryLocation);
            (properties).put(InitializerParams.Params.getMetaDataRepositoryDBname(), this.initializer.MetaDataRepositoryDBname);
            (properties).put(InitializerParams.Params.getMetaDataRepositoryUname(), this.initializer.MetaDataRepositoryUname);
            (properties).put(InitializerParams.Params.getMetaDataRepositoryPass(), this.initializer.MetaDataRepositoryPass);
        }
        String tempPassword = (String)tempProps.get(InitializerParams.Params.getWAClusterPassword());
        final String lKEY = (String)tempProps.get(InitializerParams.Params.getLicenceKey());
        final String pKEY = (String)tempProps.get(InitializerParams.Params.getProductKey());
        final String compName = (String)tempProps.get(InitializerParams.Params.getCompanyName());
        boolean authorized = this.initializer.authenticate(tempPassword);
        if (!authorized) {
            dataInFileIsIncorrect = true;
        }
        while (!authorized) {
            NodeStartUp.logger.error("Password in file did not match the in-memory copy");
            tempPassword = null;
            tempPassword = this.takePassword(cName);
            authorized = this.initializer.authenticate(tempPassword);
        }
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("Verified");
        }
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("Writing out new Interface");
        }
        String intfs = (String)tempProps.get(InitializerParams.Params.getInterfaces());
        if (intfs != null) {
            if (intfs.isEmpty()) {
                intfs = this.bindingInterface;
                dataInFileIsIncorrect = true;
            }
            else if (!intfs.contains(this.bindingInterface)) {
                intfs = intfs + "," + this.bindingInterface;
                dataInFileIsIncorrect = true;
            }
        }
        else {
            intfs = this.bindingInterface;
            dataInFileIsIncorrect = true;
        }
        if (dataInFileIsIncorrect) {
            properties.put(InitializerParams.Params.getInterfaces(), intfs);
            properties.put(InitializerParams.Params.getWAClusterName(), cName);
            properties.put(InitializerParams.Params.getWAClusterPassword(), tempPassword);
            properties.put(InitializerParams.Params.getLicenceKey(), lKEY);
            properties.put(InitializerParams.Params.getProductKey(), pKEY);
            properties.put(InitializerParams.Params.getCompanyName(), compName);
        }
        if (!properties.isEmpty()) {
            this.writePropertiesToFile(properties);
        }
        else if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("@startUpFileExistsAndObjectInMemory : No new properties are created.");
        }
        startUpMap.unlock(cName);
    }
    
    public void startUpFileExistsAndObjectNotInMemory(final String cName, final Properties tempProps, final IMap<String, MetaInfo.Initializer> startUpMap, final boolean runningWithDB, final boolean writeOutInterfaces) {
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("Startup file exists and no In-memory object");
        }
        boolean dataInFileIsIncorrect = false;
        final String tempPassword = (String)tempProps.get(InitializerParams.Params.getWAClusterPassword());
        final String dbLoc = (String)tempProps.get(InitializerParams.Params.getMetaDataRepositoryLocation());
        final String dbName = (String)tempProps.get(InitializerParams.Params.getMetaDataRepositoryDBname());
        final String dbUname = (String)tempProps.get(InitializerParams.Params.getMetaDataRepositoryUname());
        final String dbPass = (String)tempProps.get(InitializerParams.Params.getMetaDataRepositoryPass());
        final String interfaces = (String)tempProps.get(InitializerParams.Params.getInterfaces());
        final String lKEY = (String)tempProps.get(InitializerParams.Params.getLicenceKey());
        final String pKEY = (String)tempProps.get(InitializerParams.Params.getProductKey());
        final String compName = (String)tempProps.get(InitializerParams.Params.getCompanyName());
        final Properties properties = new Properties();
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("Writing out new Interface");
        }
        String intfs = (String)tempProps.get(InitializerParams.Params.getInterfaces());
        if (intfs != null) {
            if (intfs.isEmpty()) {
                intfs = this.bindingInterface;
                dataInFileIsIncorrect = true;
            }
            else if (!intfs.contains(this.bindingInterface)) {
                intfs = intfs + "," + this.bindingInterface;
                dataInFileIsIncorrect = true;
            }
        }
        else {
            intfs = this.bindingInterface;
            dataInFileIsIncorrect = true;
        }
        if (runningWithDB) {
            final MetaDataDBDetails dbChecking = new MetaDataDBDetails();
            boolean dbpresent;
            try {
                dbpresent = dbChecking.createConnection(dbLoc, dbName, dbUname, dbPass);
            }
            catch (SQLException e) {
                NodeStartUp.logger.error(e.getMessage(), (Throwable)e);
                dbpresent = false;
            }
            Map<String, String> dbDetails = null;
            if (!dbpresent) {
                try {
                    dbDetails = dbChecking.takeDBDetails();
                    dataInFileIsIncorrect = true;
                }
                catch (IOException e2) {
                    NodeStartUp.logger.error(("Problem with getting DB details from command line : " + e2.getMessage()));
                }
            }
            dbChecking.closeConnection();
            if (NodeStartUp.logger.isInfoEnabled()) {
                NodeStartUp.logger.info("got the db details right");
            }
            if (!dbpresent) {
                (this.initializer = new MetaInfo.Initializer()).construct(cName, tempPassword, dbDetails.get(InitializerParams.Params.getMetaDataRepositoryLocation()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryDBname()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryUname()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryPass()), pKEY, compName, lKEY);
                properties.putAll(dbDetails);
            }
            else {
                (this.initializer = new MetaInfo.Initializer()).construct(cName, tempPassword, dbLoc, dbName, dbUname, dbPass, pKEY, compName, lKEY);
                if (dataInFileIsIncorrect) {
                    properties.put(InitializerParams.Params.getMetaDataRepositoryLocation(), dbLoc);
                    properties.put(InitializerParams.Params.getMetaDataRepositoryDBname(), dbName);
                    properties.put(InitializerParams.Params.getMetaDataRepositoryUname(), dbUname);
                    properties.put(InitializerParams.Params.getMetaDataRepositoryPass(), dbPass);
                }
            }
        }
        else {
            (this.initializer = new MetaInfo.Initializer()).construct(cName, tempPassword, dbLoc, dbName, dbUname, dbPass, pKEY, compName, lKEY);
        }
        if (dataInFileIsIncorrect) {
            properties.put(InitializerParams.Params.getInterfaces(), intfs);
            properties.put(InitializerParams.Params.getWAClusterName(), cName);
            properties.put(InitializerParams.Params.getWAClusterPassword(), tempPassword);
            properties.put(InitializerParams.Params.getLicenceKey(), lKEY);
            properties.put(InitializerParams.Params.getProductKey(), pKEY);
            properties.put(InitializerParams.Params.getCompanyName(), compName);
        }
        if (!properties.isEmpty()) {
            this.writePropertiesToFile(properties);
        }
        else if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("@startUpFileExistsAndObjectNotInMemory : No new Properties created");
        }
        startUpMap.put(cName, this.initializer);
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("start up map : " + startUpMap.entrySet()));
        }
    }
    
    public void noStartUpFileExistsAndObjectInMemory(final String cName, final Map<String, String> clusterDetails, final boolean runningWithDB, final IMap<String, MetaInfo.Initializer> startUpMap) {
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("In-memory object available");
        }
        this.initializer = (MetaInfo.Initializer)startUpMap.get(cName);
        String tempPassword = clusterDetails.get(InitializerParams.Params.getWAClusterPassword());
        for (boolean authorized = this.initializer.authenticate(cName, tempPassword); !authorized; authorized = this.initializer.authenticate(tempPassword)) {
            NodeStartUp.logger.error("Invalid password");
            tempPassword = null;
            tempPassword = this.takePassword(cName);
        }
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("Verified to join " + cName));
        }
        final Properties properties = new Properties();
        properties.put(InitializerParams.Params.getWAClusterName(), cName);
        properties.put(InitializerParams.Params.getWAClusterPassword(), tempPassword);
        properties.put(InitializerParams.Params.getInterfaces(), this.bindingInterface);
        properties.put(InitializerParams.Params.getProductKey(), this.initializer.ProductKey);
        properties.put(InitializerParams.Params.getLicenceKey(), this.initializer.LicenseKey);
        properties.put(InitializerParams.Params.getCompanyName(), this.initializer.CompanyName);
        if (runningWithDB) {
            properties.put(InitializerParams.Params.getMetaDataRepositoryLocation(), this.initializer.MetaDataRepositoryLocation);
            properties.put(InitializerParams.Params.getMetaDataRepositoryDBname(), this.initializer.MetaDataRepositoryDBname);
            properties.put(InitializerParams.Params.getMetaDataRepositoryUname(), this.initializer.MetaDataRepositoryUname);
            properties.put(InitializerParams.Params.getMetaDataRepositoryPass(), this.initializer.MetaDataRepositoryPass);
        }
        this.writePropertiesToFile(properties);
    }
    
    public void noStartUpFileExistsAndObjectNotInMemory(final Map<String, String> clusterDetails, final boolean runningWithDB, final IMap<String, MetaInfo.Initializer> startUpMap) {
        final Properties properties = new Properties();
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info("No In-memory object");
        }
        properties.putAll(clusterDetails);
        properties.put(InitializerParams.Params.getInterfaces(), this.bindingInterface);
        if (runningWithDB) {
            Map<String, String> dbDetails = null;
            final MetaDataDBDetails dbChecking = new MetaDataDBDetails();
            try {
                dbDetails = dbChecking.takeDBDetails();
            }
            catch (IOException e) {
                NodeStartUp.logger.error(("Problem with writing out new properties(cluster details) to disk : " + e.getMessage()));
            }
            if (NodeStartUp.logger.isInfoEnabled()) {
                NodeStartUp.logger.info("got the db details right");
            }
            dbChecking.closeConnection();
            properties.putAll(dbDetails);
            (this.initializer = new MetaInfo.Initializer()).construct(clusterDetails.get(InitializerParams.Params.getWAClusterName()), clusterDetails.get(InitializerParams.Params.getWAClusterPassword()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryLocation()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryDBname()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryUname()), dbDetails.get(InitializerParams.Params.getMetaDataRepositoryPass()), clusterDetails.get(InitializerParams.Params.getProductKey()), clusterDetails.get(InitializerParams.Params.getCompanyName()), clusterDetails.get(InitializerParams.Params.getLicenceKey()));
        }
        else {
            (this.initializer = new MetaInfo.Initializer()).construct(clusterDetails.get(InitializerParams.Params.getWAClusterName()), clusterDetails.get(InitializerParams.Params.getWAClusterPassword()), null, null, null, null, clusterDetails.get(InitializerParams.Params.getProductKey()), clusterDetails.get(InitializerParams.Params.getCompanyName()), clusterDetails.get(InitializerParams.Params.getLicenceKey()));
        }
        this.writePropertiesToFile(properties);
        startUpMap.put(clusterDetails.get(InitializerParams.Params.getWAClusterName()), this.initializer);
        this.initializer.currentData();
        if (NodeStartUp.logger.isInfoEnabled()) {
            NodeStartUp.logger.info(("start up map : " + startUpMap.entrySet()));
        }
    }
    
    public String takePassword(final String name) {
        String password = null;
        int count = 0;
        boolean verified = false;
        try {
            String pass1 = "";
            while (pass1.equals("")) {
                pass1 = ConsoleReader.readPassword("Enter Cluster Password for Cluster " + name + " : ");
                if (pass1 == null) {
                    System.err.println("No Standard Input available, hence terminating.");
                    System.exit(0);
                }
            }
            password = encrypt(pass1, name);
            while (count < 3) {
                final String pass2 = ConsoleReader.readPassword("Re-enter the Password : ");
                if (pass1.equals(pass2)) {
                    if (NodeStartUp.logger.isInfoEnabled()) {
                        NodeStartUp.logger.info("Accepted");
                    }
                    verified = true;
                    break;
                }
                System.out.println("Password did not match");
                verified = false;
                ++count;
            }
            if (!verified) {
                NodeStartUp.logger.error("Cluster Password was re-entered incorrectly 3 times, please restart the system");
                System.exit(1);
            }
        }
        catch (IOException e) {
            NodeStartUp.logger.error(e.getLocalizedMessage());
        }
        return password;
    }
    
    public Map<String, String> takeClusterDetails() {
        String pass = null;
        String password = null;
        pass = System.getProperty("com.datasphere.config.password");
        final String clusterName = getClusterName();
        if (pass != null && !pass.isEmpty()) {
            password = encrypt(pass, clusterName);
        }
        else {
            System.out.println("Required property \"Cluster Password\" is undefined");
            password = this.takePassword(clusterName);
        }
        final Map<String, String> clusterDetails = new HashMap<String, String>();
        clusterDetails.put(InitializerParams.Params.getWAClusterName(), clusterName);
        clusterDetails.put(InitializerParams.Params.getWAClusterPassword(), password);
        return clusterDetails;
    }
    
    public static String getClusterName() {
        String clusterName = System.getProperty("com.datasphere.config.clusterName");
        if (clusterName != null && !clusterName.isEmpty()) {
            if (NodeStartUp.logger.isInfoEnabled()) {
                NodeStartUp.logger.info(("Using cluster name : " + clusterName + ", passed through system properties"));
            }
        }
        else {
            System.out.println("Required property \"Cluster Name\" is undefined");
            try {
                final String username = System.getProperty("user.name");
                final String defaultClause = (username != null && !username.isEmpty()) ? ("[default " + username + " (Press Enter/Return to default)]") : "";
                clusterName = ConsoleReader.readLineRespond("Enter Cluster Name " + defaultClause + " : ");
                if (clusterName == null || clusterName.isEmpty()) {
                    if (username == null || username.isEmpty()) {
                        System.out.println("Cluster Name was not provided, please restart the system.");
                        System.exit(0);
                    }
                    else {
                        clusterName = username;
                    }
                }
            }
            catch (IOException e) {
                NodeStartUp.logger.error(e.getLocalizedMessage());
            }
        }
        return clusterName;
    }
    
    public Map<String, String> takeLicenseKeyInfo(final Properties props) throws IOException {
        String clusterName = (String)props.get(InitializerParams.Params.getWAClusterName());
        if (clusterName == null || clusterName.isEmpty()) {
            clusterName = getClusterName();
        }
        String companyName = System.getProperty("com.datasphere.config.company_name");
        String licenseKey = System.getProperty("com.datasphere.config.license_key");
        String okProductKey = System.getProperty("com.datasphere.config.product_key");
        final Path path = Paths.get(getPlatformHome(), new String[0]);
        final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
        final FileTime creationTime = attributes.lastModifiedTime();
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final String date = dateFormat.format(creationTime.toMillis());
        long ctime = 0L;
        try {
            ctime = dateFormat.parse(date).getTime();
        }
        catch (ParseException e1) {
            NodeStartUp.logger.error(e1.getLocalizedMessage());
        }
        if (companyName == null || companyName.isEmpty()) {
            companyName = (String)props.get(InitializerParams.Params.getCompanyName());
        }
        if (companyName == null || companyName.isEmpty()) {
            System.out.println("Required property \"Company Name\" is undefined");
            companyName = ConsoleReader.readLineRespond("Enter Company Name : ");
        }
        if (okProductKey == null || okProductKey.isEmpty()) {
            okProductKey = (String)props.get(InitializerParams.Params.getProductKey());
        }
        if (okProductKey == null || okProductKey.isEmpty()) {
            okProductKey = LicenseManager.get().createProductKey(companyName, clusterName, ctime);
        }
        System.out.println("Product Key " + okProductKey + " registered to " + companyName);
        final Map<String, String> licenseKeyDetails = new HashMap<String, String>();
        if (licenseKey == null || licenseKey.isEmpty()) {
            licenseKey = (String)props.get(InitializerParams.Params.getLicenceKey());
        }
        if (licenseKey == null || licenseKey.isEmpty()) {
            licenseKey = ConsoleReader.readLine("Enter License Key : (will generate trial license key if left empty). ");
            if (licenseKey == null || licenseKey.isEmpty()) {
                licenseKey = LicenseManager.get().createTrialLicenseKey(clusterName, okProductKey, null);
            }
        }
        try {
            final LicenseManager lm = LicenseManager.get();
            LicenseManager.testLicense(lm, "Test", companyName, okProductKey, licenseKey, null, false);
            if (!lm.allowsOption(LicenseManager.Option.CompanyLicense)) {
                throw new RuntimeException();
            }
        }
        catch (RuntimeException ignored) {
            try {
                LicenseManager.testLicense(LicenseManager.get(), "Test", clusterName, okProductKey, licenseKey, null, false);
            }
            catch (RuntimeException e2) {
                NodeStartUp.logger.error(e2.getMessage());
                System.out.println(e2.getMessage());
                System.exit(1);
            }
        }
        licenseKeyDetails.put(InitializerParams.Params.getProductKey(), okProductKey);
        licenseKeyDetails.put(InitializerParams.Params.getLicenceKey(), licenseKey);
        licenseKeyDetails.put(InitializerParams.Params.getCompanyName(), companyName);
        return licenseKeyDetails;
    }
    
    public boolean isNodeStarted() {
        return this.nodeStarted;
    }
    
    public void setNodeStarted(final boolean nodeStarted) {
        this.nodeStarted = nodeStarted;
    }
    
    static {
        NodeStartUp.logger = Logger.getLogger((Class)NodeStartUp.class);
        NodeStartUp.platformHome = null;
    }
    
    public static class InitializerParams
    {
        public enum Params
        {
            WAClusterName, 
            WAClusterPassword, 
            MetaDataRepositoryLocation, 
            MetaDataRepositoryDBname, 
            MetaDataRepositoryUname, 
            MetaDataRepositoryPass, 
            Interfaces, 
            ProductKey, 
            LicenceKey, 
            CompanyName;
            
            public static String getWAClusterName() {
                return Params.WAClusterName.toString();
            }
            
            public static String getWAClusterPassword() {
                return Params.WAClusterPassword.toString();
            }
            
            public static String getMetaDataRepositoryLocation() {
                return Params.MetaDataRepositoryLocation.toString();
            }
            
            public static String getMetaDataRepositoryDBname() {
                return Params.MetaDataRepositoryDBname.toString();
            }
            
            public static String getMetaDataRepositoryUname() {
                return Params.MetaDataRepositoryUname.toString();
            }
            
            public static String getMetaDataRepositoryPass() {
                return Params.MetaDataRepositoryPass.toString();
            }
            
            public static String getInterfaces() {
                return Params.Interfaces.toString();
            }
            
            public static String getProductKey() {
                return Params.ProductKey.toString();
            }
            
            public static String getLicenceKey() {
                return Params.LicenceKey.toString();
            }
            
            public static String getCompanyName() {
                return Params.CompanyName.toString();
            }
        }
    }
}
