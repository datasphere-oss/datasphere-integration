package com.datasphere.mongodbreader.utils;

import com.mongodb.*;
import com.mongodb.MongoClient;

import org.bson.*;
import java.util.*;
import com.mongodb.client.*;

public class Utilities
{
    private static final int TIMEOUT = 10000;
    private static final HashSet<String> requiresUserName;
    private static final HashSet<String> requiresBoth;
    private static HashMap<String, com.mongodb.ReadPreference> readPreferences;
    
    public static MongoClient connectClient(final String hostUrls, final MongoCredential credential, final String readPreference, final Boolean sslEnabled) {
        final MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder().connectTimeout(10000).socketTimeout(10000).serverSelectionTimeout(10000);
        if (sslEnabled) {
            optionsBuilder.sslEnabled((boolean)sslEnabled).sslInvalidHostNameAllowed(true);
        }
        com.mongodb.ReadPreference pref;
        if (readPreference == null || (pref = Utilities.readPreferences.get(readPreference.toUpperCase())) == null) {
            pref = Utilities.readPreferences.get("PRIMARYPREFERRED");
        }
        optionsBuilder.readPreference(pref);
        final MongoClientOptions options = optionsBuilder.build();
        final List<ServerAddress> addrList = new ArrayList<ServerAddress>();
        for (final String hostUrl : hostUrls.split(",")) {
            final String[] temp = hostUrl.split(":");
            final String host = temp[0];
            final int port = (temp.length < 2) ? 27017 : Integer.parseInt(temp[1].trim());
            addrList.add(new ServerAddress(host, port));
        }
        if (credential != null) {
            return new MongoClient((List)addrList, credential, options);
        }
        return new MongoClient((List)addrList, options);
    }
    
    public static MongoDatabase connectDb(final MongoClient client, final String databaseName) {
        return client.getDatabase(databaseName);
    }
    
    public static MongoCollection<Document> connectCollection(final MongoDatabase database, final String collectionName) {
        return (MongoCollection<Document>)database.getCollection(collectionName);
    }
    
    public static Set<String> getCollectionSet(final MongoDatabase database, final String includeCollections, final String excludeCollections) throws Exception {
        final Set<String> collectionSet = new LinkedHashSet<String>();
        final String[] includedCollections = includeCollections.split(",");
        String[] excludedCollections = new String[0];
        if (excludeCollections != null && !excludeCollections.trim().isEmpty()) {
            excludedCollections = excludeCollections.split(",");
        }
        for (final String coll : database.listCollectionNames()) {
            Boolean exclude = false;
            for (String prefix : excludedCollections) {
                if (prefix.trim().endsWith("$")) {
                    prefix = prefix.trim().substring(0, prefix.length() - 1);
                    if (coll.startsWith(prefix)) {
                        exclude = true;
                    }
                }
                else if (coll.equals(prefix)) {
                    exclude = true;
                }
            }
            if (exclude) {
                continue;
            }
            for (String prefix : includedCollections) {
                if (prefix.trim().endsWith("$")) {
                    prefix = prefix.trim().substring(0, prefix.length() - 1);
                    if (coll.startsWith(prefix)) {
                        collectionSet.add(coll);
                    }
                }
                else if (coll.equals(prefix)) {
                    collectionSet.add(coll);
                }
            }
        }
        return collectionSet;
    }
    
    public static MongoCredential validateAuth(final String authType, String authDB, final String userName, final String password) {
        if (authType == null || authType.equalsIgnoreCase("NoAuth")) {
            return null;
        }
        if (Utilities.requiresBoth.contains(authType.toUpperCase())) {
            if (userName == null || password == null) {
                throw new IllegalArgumentException("UserName and Password are required for the authType - " + authType);
            }
        }
        else {
            if (!Utilities.requiresUserName.contains(authType.toUpperCase())) {
                throw new IllegalArgumentException("Invalid authType. Allowed values : NoAuth, Default, SCRAMSHA1, MONGODBCR, GSSAPI, PLAIN, MONGODBX509");
            }
            if (userName == null) {
                throw new IllegalArgumentException("UserName is required for the authType - " + authType);
            }
        }
        if (authDB == null || authDB.isEmpty()) {
            authDB = "admin";
        }
        final String upperCase = authType.toUpperCase();
        switch (upperCase) {
            case "DEFAULT": {
                return MongoCredential.createCredential(userName, authDB, password.toCharArray());
            }
            case "SCRAMSHA1": {
                return MongoCredential.createScramSha1Credential(userName, authDB, password.toCharArray());
            }
            case "MONGODBCR": {
                return MongoCredential.createMongoCRCredential(userName, authDB, password.toCharArray());
            }
            case "PLAIN": {
                return MongoCredential.createPlainCredential(userName, authDB, password.toCharArray());
            }
            case "GSSAPI": {
                return MongoCredential.createGSSAPICredential(userName);
            }
            case "MONGODBX509": {
                return MongoCredential.createMongoX509Credential(userName);
            }
            default: {
                return null;
            }
        }
    }
    
    static {
        requiresUserName = new HashSet<String>() {
            private static final long serialVersionUID = 1L;
            
            {
                this.add("GSSAPI");
                this.add("MONGODBX509");
            }
        };
        requiresBoth = new HashSet<String>() {
            private static final long serialVersionUID = 1L;
            
            {
                this.add("DEFAULT");
                this.add("SCRAMSHA1");
                this.add("MONGODBCR");
                this.add("PLAIN");
            }
        };
        Utilities.readPreferences = new HashMap<String, com.mongodb.ReadPreference>() {
            private static final long serialVersionUID = 1L;
            
            {
                this.put("PRIMARY", com.mongodb.ReadPreference.primary());
                this.put("PRIMARYPREFERRED", com.mongodb.ReadPreference.primaryPreferred());
                this.put("SECONDARY", com.mongodb.ReadPreference.secondary());
                this.put("SECONDARYPREFERRED", com.mongodb.ReadPreference.secondaryPreferred());
                this.put("NEAREST", com.mongodb.ReadPreference.nearest());
            }
        };
    }
}
