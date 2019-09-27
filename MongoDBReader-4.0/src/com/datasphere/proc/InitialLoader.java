package com.datasphere.proc;

import java.util.HashMap;
import java.util.Iterator;
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
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

class InitialLoader
{
    private static Logger logger;
    private MongoClient client;
    private Consumer<JsonNodeEvent> callback;
    private Map<String, List<String>> collectionMap;
    private int retryCount;
    private int maxRetries;
    private int retryInterval;
    private boolean returnPositions;
    Map<String, BsonTimestamp> CDCStartPositions;
    MongoDatabase local;
    MongoCollection<Document> oplog;
    
    public InitialLoader(final MongoClient client, final Map<String, List<String>> collectionMap, final Consumer<JsonNodeEvent> callback, final int maxRetries, final int retryInterval, final Boolean returnPositions) {
        this.client = client;
        this.collectionMap = collectionMap;
        this.callback = callback;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.retryCount = 0;
        this.returnPositions = returnPositions;
    }
    
    public Map<String, BsonTimestamp> start() {
        if (this.returnPositions) {
            this.CDCStartPositions = new HashMap<String, BsonTimestamp>();
            this.local = Utilities.connectDb(this.client, "local");
            this.oplog = Utilities.connectCollection(this.local, "oplog.rs");
        }
        for (final String dbName : this.collectionMap.keySet()) {
            final MongoDatabase database = Utilities.connectDb(this.client, dbName);
            final List<String> collectionList = this.collectionMap.get(dbName);
            int i = 0;
            while (i < collectionList.size()) {
                try {
                    this.loadCollection(dbName, database, collectionList.get(i));
                }
                catch (Exception e) {
                    if ((e instanceof MongoSocketException || e instanceof MongoClientException) && this.retryCount < this.maxRetries) {
                        try {
                            Thread.sleep(this.retryInterval * 1000);
                        }
                        catch (InterruptedException ex) {}
                        ++this.retryCount;
                        InitialLoader.logger.info((Object)("Encountered mongo exception, attempting connection retry, retry count : " + this.retryCount));
                        continue;
                    }
                    throw e;
                }
                ++i;
            }
        }
        return this.CDCStartPositions;
    }
    
    private void loadCollection(final String databaseName, final MongoDatabase database, final String collectionName) {
        final MongoCollection<Document> collection = Utilities.connectCollection(database, collectionName);
        InitialLoader.logger.info((Object)("Starting Initial Load : " + databaseName + "." + collectionName));
        final long stime = System.currentTimeMillis();
        if (this.returnPositions) {
            final BasicDBObject findquery = new BasicDBObject();
            findquery.put("ns", (Object)new BasicDBObject("$eq", (Object)(databaseName + "." + collectionName)));
            final Document lastDoc = (Document)this.oplog.find((Bson)findquery).sort((Bson)new BasicDBObject("$natural", (Object)(-1))).first();
            if (lastDoc != null) {
                this.CDCStartPositions.put(databaseName + "." + collectionName, (BsonTimestamp)lastDoc.get((Object)"ts"));
            }
        }
        final Iterator<Document> allDocs = (Iterator<Document>)collection.find().iterator();
        int numDocs = 0;
        while (allDocs.hasNext()) {
            final Document doc = allDocs.next();
            JsonNodeEvent jne = new JsonNodeEvent(System.currentTimeMillis());
            jne.data = BuiltInFunc.JSONFrom((Object)doc.toJson());
            jne.metadata = new HashMap();
            jne.metadata.put("DatabaseName", databaseName);
            jne.metadata.put("CollectionName", collectionName);
            jne.metadata.put("TimeStamp", System.currentTimeMillis());
            jne.metadata.put("NameSpace", databaseName + "." + collectionName);
            jne.metadata.put("DocumentKey", jne.data.get("_id"));
            jne.metadata.put("OperationName", "SELECT");
            this.callback.accept(jne);
            ++numDocs;
        }
        final long etime = System.currentTimeMillis();
        final long tdiff = etime - stime;
        final double rate = numDocs * 1000.0 / (tdiff * 1.0);
        InitialLoader.logger.info((Object)("Performed Intial Load of [" + databaseName + "." + collectionName + "] " + numDocs + " docs in " + tdiff + "ms rate " + rate + " doc/s"));
    }
    
    static {
        InitialLoader.logger = Logger.getLogger((Class)InitialLoader.class);
    }
}
