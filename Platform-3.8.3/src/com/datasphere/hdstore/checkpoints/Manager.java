package com.datasphere.hdstore.checkpoints;

import org.apache.log4j.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.constants.*;
import com.datasphere.hdstore.exceptions.*;
import java.io.*;
import javax.xml.bind.*;
import java.security.*;
import com.fasterxml.jackson.databind.node.*;
import com.datasphere.hdstore.*;
import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.runtime.*;

public class Manager implements CheckpointManager
{
    private static final Class<Manager> thisClass;
    private static final Logger logger;
    private static final String nodeName;
    private final HDStoreManager manager;
    private final Map<String, Details> detailsMap;
    private DataType checkpointType;
    
    public Manager(final HDStoreManager manager) {
        this.detailsMap = new HashMap<String, Details>();
        this.manager = manager;
    }
    
    @Override
    public void add(final String hdStoreName, final HD hd, final Position hdPosition) {
        if (hdPosition != null && hdPosition.values() != null) {
            final Details details = this.getHDStoreDetails(hdStoreName);
            hd.put("$node_name", Manager.nodeName);
            hd.put("$checkpoint", details.timestamp);
            final Details details2 = details;
            ++details2.hds;
            if (Manager.logger.isDebugEnabled()) {
                final JsonNode hdIDJson = hd.get("$id");
                if (hdIDJson != null) {
                    final String hdID = hdIDJson.asText();
                    Manager.logger.debug((Object)String.format("HD %d in checkpoint #%d is '%s'", details.hds, details.timestamp, hdID));
                }
            }
            details.position.mergeHigherPositions(hdPosition);
        }
    }
    
    private Details getHDStoreDetails(final String hdStoreName) {
        Details result = this.detailsMap.get(hdStoreName);
        if (result == null) {
            result = new Details();
            this.detailsMap.put(hdStoreName, result);
        }
        return result;
    }
    
    @Override
    public void start(final String hdStoreName) {
        final Details details = this.getHDStoreDetails(hdStoreName);
        details.start();
        Manager.logger.debug((Object)String.format("Starting new checkpoint #%d for HDStore '%s'", details.timestamp, hdStoreName));
    }
    
    @Override
    public void flush(final HDStore hdStore) {
        final String hdStoreName = hdStore.getName();
        final Map<String, Object> properties = hdStore.getProperties();
        final Details details;
        synchronized (this) {
            if (this.checkpointType == null) {
                this.checkpointType = InternalType.CHECKPOINT.getDataType(this.manager, properties);
            }
            details = this.getCurrentCheckpoint(hdStoreName);
        }
        if (details != null && details.position != null && details.position.values() != null) {
            final Collection<Path> paths = (Collection<Path>)details.position.values();
            final HD checkpointHD = this.makeCheckpointHD(hdStoreName, paths, details);
            this.checkpointType.insert(checkpointHD);
            hdStore.fsync();
            this.checkpointType.fsync();
            if (Manager.logger.isInfoEnabled()) {
                Manager.logger.info((Object)String.format("Wrote checkpoint #%d with %d paths for HDStore '%s' for %d HDs", details.timestamp, paths.size(), hdStoreName, details.hds));
            }
        }
    }
    
    @Override
    public void remove(final String hdStoreName) {
        final String actualName = this.manager.translateName(NameType.HDSTORE, hdStoreName);
        final JsonNode deleteStatement = Utility.readTree(String.format("{ \"delete\": [ \"*\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\": {     \"oper\": \"eq\",    \"attr\": \"HDStoreName\",    \"value\": \"%s\" }}", actualName));
        this.doDelete(deleteStatement, hdStoreName);
        this.detailsMap.remove(hdStoreName);
    }
    
    private void deleteHDsPastCheckpoint(final String hdStoreName, final long timestamp) {
        final String actualName = this.manager.translateName(NameType.HDSTORE, hdStoreName);
        final JsonNode deleteStatement = Utility.readTree(String.format("{ \"delete\": [ \"*\" ],  \"from\":  [ \"%s\" ],  \"where\": {     \"and\": [    { \"oper\": \"eq\",      \"attr\": \"$node_name\",      \"value\": \"%s\" },    { \"oper\": \"gt\",      \"attr\": \"$checkpoint\",      \"value\": %d }    ]  }}", actualName, Manager.nodeName, timestamp));
        this.doDelete(deleteStatement, hdStoreName);
    }
    
    private void doDelete(final JsonNode deleteStatement, final String hdStoreName) {
        try {
            this.manager.delete(deleteStatement);
        }
        catch (HDStoreMissingException ex) {}
        catch (HDStoreException exception) {
            Manager.logger.warn((Object)String.format("Failed to remove checkpoint for HDStore '%s'", hdStoreName), (Throwable)exception);
        }
    }
    
    private HD makeCheckpointHD(final String hdStoreName, final Collection<Path> paths, final Details details) {
        final String actualName = this.manager.translateName(NameType.HDSTORE, hdStoreName);
        final HD result = new HD();
        final Path[] pathsArray = paths.toArray(new Path[paths.size()]);
        final String pathsString = Utility.serializeToBase64String(pathsArray);
        result.put("$id", getHash(Manager.nodeName + '.' + actualName));
        result.put("$timestamp", details.timestamp);
        result.put("NodeName", Manager.nodeName);
        result.put("HDStoreName", actualName);
        result.put("HDCount", details.hds);
        result.put("PathCount", paths.size());
        result.put("Paths", pathsString);
        return result;
    }
    
    private static String getHash(final String... values) {
        String result = null;
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            for (final String value : values) {
                final byte[] valueBytes = value.getBytes();
                md.update(valueBytes);
            }
            final byte[] digest = md.digest();
            result = DatatypeConverter.printHexBinary(digest);
        }
        catch (NoSuchAlgorithmException exception) {
            Manager.logger.error((Object)String.format("Cannot create hash using algorithm '%s'", "MD5"), (Throwable)exception);
        }
        return result;
    }
    
    private Details getCurrentCheckpoint(final String hdStoreName) {
        Details result = null;
        final Details details = this.detailsMap.get(hdStoreName);
        if (details != null) {
            result = new Details(details);
            details.start();
        }
        return result;
    }
    
    private long getLastTimestamp(final String hdStoreName) {
        long result = 0L;
        try {
            final String actualName = this.manager.translateName(NameType.HDSTORE, hdStoreName);
            final ObjectNode queryJson = (ObjectNode)Utility.readTree(String.format("{ \"select\":[ \"$timestamp\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\": {     \"and\": [    { \"oper\": \"eq\",      \"attr\": \"NodeName\",      \"value\": \"%s\" },    { \"oper\": \"eq\",      \"attr\": \"HDStoreName\",      \"value\": \"%s\" }    ]  },  \"orderby\": [    { \"attr\": \"$timestamp\", \"ascending\": false }  ]}", Manager.nodeName, actualName));
            final HDQuery query = this.manager.prepareQuery((JsonNode)queryJson);
            final Iterator<HD> checkpoints = query.execute().iterator();
            if (checkpoints.hasNext()) {
                final HD hd = checkpoints.next();
                result = hd.get("$timestamp").asLong(0L);
                Manager.logger.info((Object)("last timestamp read" + result));
            }
        }
        catch (HDStoreMissingException exception) {
            final String checkpointStoreName = InternalType.CHECKPOINT.getHDStoreName();
            if (!checkpointStoreName.equals(exception.hdStoreName)) {
                throw exception;
            }
        }
        return result;
    }
    
    @Override
    public Position get(final String hdStoreName) {
        PathManager result = new PathManager();
        boolean isCheckpointFound = false;
        try {
            final String actualName = this.manager.translateName(NameType.HDSTORE, hdStoreName);
            final String queryString = String.format("{ \"select\":[ \"Paths\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\":   { \"oper\": \"eq\",    \"attr\": \"HDStoreName\",    \"value\": \"%s\" } },  \"orderby\": [    { \"attr\": \"$timestamp\", \"ascending\": true }  ]}", actualName);
            final HDQuery query = this.manager.prepareQuery(Utility.readTree(queryString));
            for (final HD checkpoint : query.execute()) {
                isCheckpointFound = true;
                final String pathsString = checkpoint.get("Paths").asText();
                final Object pathsObject = Utility.serializeFromBase64String(pathsString);
                if (!(pathsObject instanceof Path[])) {
                    Manager.logger.error((Object)String.format("Malformed checkpoint for HDStore '%s': %s", hdStoreName, checkpoint));
                }
                else {
                    final Path[] pathsArray = (Path[])pathsObject;
                    if (result == null) {
                        result = new PathManager((Collection)new HashSet(Arrays.asList(pathsArray)));
                    }
                    else {
                        result.mergeHigherPositions(new Position((Set)new HashSet(Arrays.asList(pathsArray))));
                    }
                }
            }
            if (isCheckpointFound) {
                Manager.logger.info((Object)String.format("Read a valid checkpoint for HDStore '%s'", hdStoreName));
                final Details details = this.getHDStoreDetails(hdStoreName);
                details.position = result;
                if (Manager.logger.isDebugEnabled()) {
                    com.datasphere.utility.Utility.prettyPrint(details.position);
                }
            }
            else {
                Manager.logger.info((Object)String.format("Checkpoint not available for HDStore '%s'", hdStoreName));
            }
        }
        catch (HDStoreMissingException ignored) {
            Manager.logger.info((Object)String.format("Checkpoint not available for HDStore '%s'", hdStoreName));
        }
        return result.toPosition();
    }
    
    @Override
    public void removeInvalidHDs(final String hdStoreName) {
        final long lastTimestamp = this.getLastTimestamp(hdStoreName);
        final HDStore hdStore = this.manager.get(hdStoreName, null);
        if (hdStore == null) {
            return;
        }
        long deletedHDCount = hdStore.getHDCount();
        Manager.logger.info((Object)("hd count before purging any hds " + deletedHDCount));
        this.deleteHDsPastCheckpoint(hdStoreName, lastTimestamp);
        final long afterCount = hdStore.getHDCount();
        deletedHDCount -= afterCount;
        if (deletedHDCount > 0L) {
            Manager.logger.info((Object)String.format("Purged %d HDs past checkpoint #%d in HDStore '%s'", deletedHDCount, lastTimestamp, hdStoreName));
        }
    }
    
    @Override
    public void closeHDStore(final String hdStoreName) {
        this.detailsMap.remove(hdStoreName);
    }
    
    @Override
    public void writeBlankCheckpoint(final String hdStoreName) {
        final HDStore hdStore = this.manager.get(hdStoreName, null);
        final Details details = this.getHDStoreDetails(hdStoreName);
        details.position = new PathManager();
        this.flush(hdStore);
        Manager.logger.info((Object)String.format("Wrote checkpoint #%d with %d paths for HDStore '%s' for %d HDs", details.timestamp, 0, hdStoreName, details.hds));
    }
    
    static {
        thisClass = Manager.class;
        logger = Logger.getLogger((Class)Manager.thisClass);
        nodeName = BaseServer.getServerName();
    }
}
