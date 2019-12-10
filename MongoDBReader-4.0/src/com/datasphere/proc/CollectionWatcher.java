package com.datasphere.proc;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.mongodbreader.utils.Utilities;
import com.datasphere.proc.events.JsonNodeEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class CollectionWatcher extends Thread
{
    private static Logger logger;
    private volatile boolean running;
    private List<String> collectionList;
    private MongoClient client;
    private BsonTimestamp startTimestamp;
    private BsonTimestamp currentTimestamp;
    private Consumer<JsonNodeEvent> callback;
    private MongoCursor<Document> opCursor;
    private int retryCount;
    private int maxRetries;
    private int retryInterval;
    private Map<String, BsonTimestamp> CDCStartPositions;
    private boolean fullDocumentUpdateLookup;
    
    public CollectionWatcher(final MongoClient client, final List<String> collectionList, final BsonTimestamp startTimestamp, final Consumer<JsonNodeEvent> callback, final int maxRetries, final int retryInterval, final Map<String, BsonTimestamp> CDCStartPositions, final boolean fullDocumentUpdateLookup) {
        this.running = true;
        this.collectionList = collectionList;
        this.client = client;
        this.startTimestamp = startTimestamp;
        this.callback = callback;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.retryCount = 0;
        this.CDCStartPositions = CDCStartPositions;
        this.fullDocumentUpdateLookup = fullDocumentUpdateLookup;
        this.setName("MongoDBReader-CollectionWatcher");
    }
    
    @Override
    public void run() {
        Boolean initialized = false;
        Boolean doneOne = false;
        while (this.running) {
            try {
                final MongoDatabase local = Utilities.connectDb(this.client, "local");
                final MongoCollection<Document> oplog = Utilities.connectCollection(local, "oplog.rs");
                if (!initialized) {
                    BsonTimestamp lastRecordTimestamp = null;
                    final Document lastDoc = (Document)oplog.find().sort((Bson)new BasicDBObject("$natural", (Object)(-1))).first();
                    if (lastDoc == null) {
                        throw new RuntimeException("no oplog configured for this connection. Change data cannot be captured");
                    }
                    lastRecordTimestamp = (BsonTimestamp)lastDoc.get((Object)"ts");
                    if (this.startTimestamp != null) {
                        this.currentTimestamp = this.startTimestamp;
                    }
                    else {
                        this.currentTimestamp = lastRecordTimestamp;
                    }
                    initialized = true;
                }
                final BasicDBObject findquery = new BasicDBObject();
                findquery.put("ts", (Object)new BasicDBObject("$gt", (Object)this.currentTimestamp));
                findquery.put("ns", (Object)new BasicDBObject("$in", (Object)this.collectionList));
                this.opCursor = (MongoCursor<Document>)oplog.find((Bson)findquery).sort((Bson)new BasicDBObject("$natural", (Object)1)).cursorType(CursorType.TailableAwait).noCursorTimeout(true).iterator();
                while (this.running) {
                    if (!this.opCursor.hasNext()) {
                        continue;
                    }
                    final Document log = (Document)this.opCursor.next();
                    final String namespace = log.getOrDefault((Object)"ns", (Object)"").toString();
                    final String dbName = namespace.substring(0, namespace.indexOf("."));
                    final String collName = namespace.substring(namespace.indexOf(".") + 1);
                    final BsonTimestamp opTimestamp = (BsonTimestamp)log.get((Object)"ts");
                    if (this.CDCStartPositions != null && this.CDCStartPositions.get(namespace) != null && opTimestamp.compareTo((BsonTimestamp)this.CDCStartPositions.get(namespace)) <= 0) {
                        if (opTimestamp.compareTo((BsonTimestamp)this.CDCStartPositions.get(namespace)) != 0) {
                            continue;
                        }
                        this.CDCStartPositions.remove(namespace);
                    }
                    else {
                        Document doc = (Document)log.get((Object)"o");
                        final JsonNodeEvent jne = new JsonNodeEvent(System.currentTimeMillis());
                        jne.metadata = new HashMap();
                        if (doc.get((Object)"_id") == null) {
                            if (log.get((Object)"o2") != null) {
                                doc.put("_id", ((Document)log.get((Object)"o2")).get((Object)"_id"));
                            }
                            final Object _id = doc.get((Object)"_id");
                            if (_id != null) {
                                if (this.fullDocumentUpdateLookup) {
                                    doc = this.fetchFullDocument(dbName, collName, _id);
                                }
                                else if (doc.get((Object)"$set") != null) {
                                    doc = (Document)doc.get((Object)"$set");
                                    doc.put("_id", _id);
                                }
                            }
                        }
                        if (doc.get((Object)"_id") == null) {
                            CollectionWatcher.logger.warn((Object)("Encountered an invalid log for the collection - " + namespace + ". So skipping it."));
                        }
                        else {
                            jne.data = BuiltInFunc.JSONFrom((Object)doc.toJson());
                            jne.metadata.put("DatabaseName", dbName);
                            jne.metadata.put("CollectionName", collName);
                            this.currentTimestamp = opTimestamp;
                            jne.metadata.put("TimeStamp", this.currentTimestamp.getTime());
                            jne.metadata.put("NameSpace", namespace);
                            jne.metadata.put("DocumentKey", jne.data.get("_id"));
                            final String op = (String)log.get((Object)"op");
                            if (op.equals("d")) {
                                jne.metadata.put("OperationName", "DELETE");
                                JsonNode data = jne.data;
                                try {
                                    data = new ObjectMapper().readTree("{\"_id\":" + jne.data.get("_id") + "}");
                                }
                                catch (IOException ex) {}
                                jne.data = data;
                            }
                            else if (op.equals("i")) {
                                jne.metadata.put("OperationName", "INSERT");
                            }
                            else {
                                if (!op.equals("u")) {
                                    continue;
                                }
                                jne.metadata.put("OperationName", "UPDATE");
                            }
                            if (!doneOne) {
                                CollectionWatcher.logger.info((Object)("Started sending changes for the collections " + this.collectionList.toString()));
                                doneOne = true;
                            }
                            this.callback.accept(jne);
                        }
                    }
                }
                continue;
            }
            catch (Exception e) {
                if (this.opCursor != null) {
                    this.opCursor.close();
                }
                if (e instanceof MongoInterruptedException && !this.running) {
                    return;
                }
                if ((e instanceof MongoSocketException || e instanceof MongoClientException) && this.retryCount < this.maxRetries) {
                    try {
                        Thread.sleep(this.retryInterval * 1000);
                    }
                    catch (InterruptedException ex2) {}
                    ++this.retryCount;
                    CollectionWatcher.logger.info((Object)("Encountered mongo exception, attempting connection retry, retry count : " + this.retryCount));
                    continue;
                }
                throw e;
            }
        }
    }
    
    private Document fetchFullDocument(final String dbName, final String collName, final Object _id) {
        final BasicDBObject findquery = new BasicDBObject();
        findquery.put("_id", (Object)new BasicDBObject("$eq", _id));
        return (Document)this.client.getDatabase(dbName).getCollection(collName).find((Bson)findquery).first();
    }
    
    public void stopRunning() {
        this.running = false;
        if (this.opCursor != null) {
            this.opCursor.close();
        }
    }
    
    public BsonTimestamp getTimestamp() {
        return this.currentTimestamp;
    }
    
    static {
        CollectionWatcher.logger = Logger.getLogger((Class)CollectionWatcher.class);
    }
}
