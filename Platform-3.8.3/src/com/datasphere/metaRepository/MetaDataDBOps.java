package com.datasphere.metaRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.DatabaseException;

import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.uuid.UUID;

public class MetaDataDBOps
{
    private static final Logger logger;
    private static MetaDataDBOps INSTANCE;
    private static final String PU_NAME = "MetadataStore";
    public static final String LOGGING_LEVEL = "SEVERE";
    public static String DBLocation;
    public static String DBName;
    public static String DBUname;
    public static String DBPassword;
    private static volatile EntityManager instance;
    
    private MetaDataDBOps() {
        if (MetaDataDBOps.INSTANCE != null) {
            throw new IllegalStateException("An instance of MetaDataDBOps already exists");
        }
    }
    
    public static MetaDataDBOps getInstance() {
        return MetaDataDBOps.INSTANCE;
    }
    
    public static void setDBDetails(final String DBLocation, final String DBName, final String DBUname, final String DBPassword) {
        MetaDataDBOps.DBLocation = DBLocation;
        MetaDataDBOps.DBName = DBName;
        MetaDataDBOps.DBUname = DBUname;
        MetaDataDBOps.DBPassword = DBPassword;
        System.out.println("DB details : " + DBLocation + " , " + DBName + " , " + DBUname);
    }
    
    private static EntityManager initManager() {
        try {
            final Map<String, Object> properties = new HashMap<String, Object>();
            final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
            properties.put("javax.persistence.jdbc.user", MetaDataDBOps.DBUname);
            properties.put("javax.persistence.jdbc.password", MetaDataDBOps.DBPassword);
            properties.put("javax.persistence.jdbc.url", metaDataDbProvider.getJDBCURL(MetaDataDBOps.DBLocation, MetaDataDBOps.DBName, MetaDataDBOps.DBUname, MetaDataDBOps.DBPassword));
            properties.put("javax.persistence.jdbc.driver", metaDataDbProvider.getJDBCDriver());
            properties.put("eclipselink.logging.level", "SEVERE");
            properties.put("eclipselink.weaving", "STATIC");
            final EntityManagerFactory m_factory = Persistence.createEntityManagerFactory("MetadataStore", (Map)properties);
            return m_factory.createEntityManager();
        }
        catch (Exception e) {
            MetaDataDBOps.logger.error((Object)e, (Throwable)e);
            if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)("Shutting down server it couldn't connect to database with error: " + e.getMessage()));
                System.exit(1);
            }
            throw e;
        }
    }
    
    public static EntityManager getManager() {
        if (MetaDataDBOps.instance == null) {
            synchronized (MetaDataDBOps.class) {
                if (MetaDataDBOps.instance == null) {
                    MetaDataDBOps.instance = initManager();
                }
            }
        }
        return MetaDataDBOps.instance;
    }
    
    private Object cloneObject(final Object obj) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b;
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            b = bos.toByteArray();
        }
        catch (IOException e2) {
            b = null;
        }
        Object retObj = null;
        if (b != null) {
            final ByteArrayInputStream bis = new ByteArrayInputStream(b);
            try {
                final ObjectInputStream ois = new ObjectInputStream(bis);
                retObj = ois.readObject();
            }
            catch (ClassNotFoundException | IOException ex2) {
                retObj = null;
            }
        }
        return retObj;
    }
    
    public synchronized boolean store(MetaInfo.MetaObject mObject) {
        long opStartTime = 0L;
        long opEndTime = 0L;
        if (!Server.persistenceIsEnabled()) {
            return false;
        }
        EntityTransaction txn = null;
        mObject.setUri(mObject.getUri());
        mObject = (MetaInfo.MetaObject)this.cloneObject(mObject);
        if (mObject.type.ordinal() == EntityType.APPLICATION.ordinal()) {
            mObject.type = EntityType.FLOW;
        }
        final EntityManager m = getManager();
        try {
            if (MetaDataDBOps.logger.isInfoEnabled()) {
                opStartTime = System.currentTimeMillis();
            }
            synchronized (m) {
                m.clear();
                txn = m.getTransaction();
                txn.begin();
                if (MetaDataDBOps.logger.isTraceEnabled()) {
                    MetaDataDBOps.logger.trace((Object)("Store object " + mObject.getUri() + " = " + mObject + " in DB"));
                }
                m.merge((Object)mObject);
                m.flush();
                txn.commit();
            }
        }
        catch (Exception e) {
            MetaDataDBOps.logger.error((Object)("Problem storing object " + mObject.getUri() + " = " + mObject + " in DB"), (Throwable)e);
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error ", (Throwable)e);
                System.exit(1);
            }
            else {
                MetaDataDBOps.logger.error((Object)"Error while writing to metadata database", (Throwable)e);
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
            }
            throw e;
        }
        finally {
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                opEndTime = System.currentTimeMillis();
                MetaDataDBOps.logger.info((Object)("DB Operation for put of " + mObject.getFullName() + " took " + (opEndTime - opStartTime) / 1000L + " seconds"));
            }
        }
        return true;
    }
    
    public Object get(final EntityType eType, final UUID uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfGet get) throws MetaDataDBOpsException {
        Object result = null;
        switch (get) {
            case BY_UUID: {
                result = this.loadByUUID(uuid);
                break;
            }
            case BY_NAME: {
                result = this.loadByName(eType, namespace, name, version);
                break;
            }
            case BY_ENTITY_TYPE: {
                result = this.loadByEntityType(eType);
                break;
            }
            case BY_NAMESPACE: {
                result = this.loadByNamespace(namespace);
                break;
            }
            case BY_NAMESPACE_AND_ENTITY_TYPE: {
                result = this.loadByEntityTypeInNamespace(namespace, eType);
                break;
            }
            case BY_ALL: {
                result = this.loadAll();
                break;
            }
        }
        return result;
    }
    
    public Integer remove(final EntityType eType, final UUID uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfRemove remove) throws MetaDataDBOpsException {
        Integer result = 0;
        switch (remove) {
            case BY_UUID: {
                result = this.removeByUUID(uuid);
                break;
            }
            case BY_NAME: {
                result = this.removeByName(eType, namespace, name, version);
                break;
            }
        }
        return result;
    }
    
    public boolean checkIfFlowIsApplication(final MetaInfo.MetaObject flow) {
        return flow.getUri().contains(":APPLICATION:");
    }
    
    public MetaInfo.MetaObject convertFlowToApplication(MetaInfo.MetaObject flow) {
        final MetaInfo.MetaObject oldmeta = flow;
        try {
            flow = oldmeta.clone();
        }
        catch (CloneNotSupportedException ex) {}
        flow.type = EntityType.APPLICATION;
        return flow;
    }
    
    private synchronized List<MetaInfo.MetaObject> loadAll() {
        List<MetaInfo.MetaObject> resultMetaObjects = null;
        try {
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)"Load All");
            }
            final EntityManager manager = getManager();
            synchronized (manager) {
                final String query = "Select w from MetaInfo$MetaObject w ";
                final Query q = manager.createQuery(query);
                final List<?> resultList = (List<?>)q.getResultList();
                if (resultList != null) {
                    if (MetaDataDBOps.logger.isDebugEnabled()) {
                        MetaDataDBOps.logger.debug((Object)("Found " + resultList.size() + " objects in Database."));
                    }
                    resultMetaObjects = new ArrayList<MetaInfo.MetaObject>();
                    for (final Object o : resultList) {
                        final MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)o;
                        if (metaObject.getType().ordinal() == EntityType.FLOW.ordinal()) {
                            if (this.checkIfFlowIsApplication(metaObject)) {
                                final MetaInfo.MetaObject converted = this.convertFlowToApplication(metaObject);
                                resultMetaObjects.add(converted);
                            }
                            else {
                                resultMetaObjects.add(metaObject);
                            }
                        }
                        else {
                            resultMetaObjects.add(metaObject);
                        }
                    }
                }
                else if (MetaDataDBOps.logger.isDebugEnabled()) {
                    MetaDataDBOps.logger.debug((Object)"No Objects found in Database.");
                }
            }
        }
        catch (Exception e) {
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
        }
        return resultMetaObjects;
    }
    
    private synchronized MetaInfo.MetaObject loadByUUID(final UUID key) throws MetaDataDBOpsException {
        MetaInfo.MetaObject mObject = null;
        try {
            MDConstants.checkNullParams("Can't load MetaObject with NULL UUID", key);
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)("Load Meta Object by UUID " + key));
            }
            final EntityManager manager = getManager();
            synchronized (manager) {
                final String query = "Select w from MetaInfo$MetaObject w where w.uuid = :uuid";
                final TypedQuery<Object[]> q = (TypedQuery<Object[]>)manager.createQuery(query, (Class)Object[].class);
                q.setParameter("uuid", (Object)key);
                final List<?> wtypes = (List<?>)q.getResultList();
                if (wtypes.size() == 1) {
                    mObject = (MetaInfo.MetaObject)wtypes.get(0);
                    if (MetaDataDBOps.logger.isDebugEnabled()) {
                        MetaDataDBOps.logger.debug((Object)("Loaded MetaObject by UUID" + key + " = " + mObject.getUuid()));
                    }
                }
                else if (MetaDataDBOps.logger.isDebugEnabled()) {
                    MetaDataDBOps.logger.debug((Object)("Could not load MetaObject by UUID " + key));
                }
                if (mObject instanceof MetaInfo.Flow) {
                    final boolean isApp = this.checkIfFlowIsApplication(mObject);
                    if (isApp) {
                        mObject = this.convertFlowToApplication(mObject);
                    }
                }
            }
        }
        catch (Exception e) {
            if (e instanceof MetaDataDBOpsException) {
                throw new MetaDataDBOpsException(e.getMessage());
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
        }
        return mObject;
    }
    
    private MetaInfo.MetaObject loadByName(final EntityType eType, final String namespace, final String name, final Integer version) throws MetaDataDBOpsException {
        try {
            MDConstants.checkNullParams("Can't load MetaObject with NULL values for either of, Entity Type: " + eType + ", Namespace: " + namespace + ", name: " + name, eType, namespace, name);
            List<MetaInfo.MetaObject> mObject = null;
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)("Load MetaObject by with name : " + name + " in namespace : " + namespace));
            }
            final EntityManager m = getManager();
            synchronized (m) {
                Query q;
                if (version == null) {
                    final String query = "select w from MetaInfo$MetaObject w where w.name = :name AND w.type = :type AND w.nsName = :namespace ORDER BY w.version DESC";
                    q = m.createQuery(query);
                    q.setParameter("name", (Object)name);
                    if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
                        q.setParameter("type", (Object)EntityType.FLOW);
                    }
                    else {
                        q.setParameter("type", (Object)eType);
                    }
                    q.setParameter("namespace", (Object)namespace);
                }
                else {
                    final String query = "select w from MetaInfo$MetaObject w where w.uri = :uri";
                    q = m.createQuery(query);
                    final String uriInCaps = NamePolicy.makeKey(namespace + ":" + eType + ":" + name + ":" + version);
                    q.setParameter("uri", (Object)uriInCaps);
                }
                final List<?> wtypes = (List<?>)q.getResultList();
                if (wtypes.size() == 0) {
                    if (MetaDataDBOps.logger.isDebugEnabled()) {
                        MetaDataDBOps.logger.debug((Object)("Zero object found for name" + name + ", returning null"));
                    }
                    return null;
                }
                if (MetaDataDBOps.logger.isDebugEnabled()) {
                    MetaDataDBOps.logger.debug((Object)("Loaded MetaObject by " + name));
                }
                mObject = (List<MetaInfo.MetaObject>)wtypes;
                MetaInfo.MetaObject toBeReturned = mObject.get(0);
                if (toBeReturned instanceof MetaInfo.Flow) {
                    final boolean isApp = this.checkIfFlowIsApplication(toBeReturned);
                    if (isApp) {
                        toBeReturned = this.convertFlowToApplication(toBeReturned);
                    }
                }
                return toBeReturned;
            }
        }
        catch (Exception e) {
            if (e instanceof MetaDataDBOpsException) {
                throw new MetaDataDBOpsException(e.getMessage());
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
            return null;
        }
    }
    
    private Set<MetaInfo.MetaObject> loadByEntityType(final EntityType eType) throws MetaDataDBOpsException {
        try {
            MDConstants.checkNullParams("Can't load Object by EntityType NULL", eType);
            if (eType.isStoreable()) {
                List<MetaInfo.MetaObject> resultMetaObjects = null;
                if (MetaDataDBOps.logger.isDebugEnabled()) {
                    MetaDataDBOps.logger.debug((Object)("Load by Entity Type : " + eType.toString()));
                }
                final EntityManager manager = getManager();
                synchronized (manager) {
                    final String query = "select w from MetaInfo$MetaObject w where w.type = :type";
                    final Query q = manager.createQuery(query);
                    if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
                        q.setParameter("type", (Object)EntityType.FLOW);
                    }
                    else {
                        q.setParameter("type", (Object)eType);
                    }
                    final List<?> resultList = (List<?>)q.getResultList();
                    if (resultList.size() == 0) {
                        if (MetaDataDBOps.logger.isDebugEnabled()) {
                            MetaDataDBOps.logger.debug((Object)("No Object of type : " + eType.toString() + " found in DB"));
                        }
                        return null;
                    }
                    resultMetaObjects = (List<MetaInfo.MetaObject>)resultList;
                    if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
                        final List<MetaInfo.MetaObject> appList = new ArrayList<MetaInfo.MetaObject>();
                        for (MetaInfo.MetaObject appMetaObject : resultMetaObjects) {
                            final boolean isApp = this.checkIfFlowIsApplication(appMetaObject);
                            if (isApp) {
                                appMetaObject = this.convertFlowToApplication(appMetaObject);
                                appList.add(appMetaObject);
                            }
                        }
                        resultMetaObjects = appList;
                    }
                    else if (eType.ordinal() == EntityType.FLOW.ordinal()) {
                        final List<MetaInfo.MetaObject> appList = new ArrayList<MetaInfo.MetaObject>();
                        for (final MetaInfo.MetaObject appMetaObject : resultMetaObjects) {
                            final boolean isApp = this.checkIfFlowIsApplication(appMetaObject);
                            if (!isApp) {
                                appList.add(appMetaObject);
                            }
                        }
                        resultMetaObjects = appList;
                    }
                    return new HashSet<MetaInfo.MetaObject>(resultMetaObjects);
                }
            }
            return null;
        }
        catch (Exception e) {
            if (e instanceof MetaDataDBOpsException) {
                throw new MetaDataDBOpsException(e.getMessage());
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
            return null;
        }
    }
    
    private Set<MetaInfo.MetaObject> loadByNamespace(final String namespace) throws MetaDataDBOpsException {
        try {
            MDConstants.checkNullParams("Can't load MetaObjects from NULL namespace", namespace);
            List<MetaInfo.MetaObject> resultMetaObjects = null;
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)("Load by Namespace : " + namespace));
            }
            final EntityManager manager = getManager();
            synchronized (manager) {
                final String query = "select w from MetaInfo$MetaObject w where w.nsName = :nsName";
                final Query q = manager.createQuery(query);
                q.setParameter("nsName", (Object)namespace);
                final List<?> resultList = (List<?>)q.getResultList();
                if (resultList.size() == 0) {
                    if (MetaDataDBOps.logger.isDebugEnabled()) {
                        MetaDataDBOps.logger.debug((Object)("No Object of type : " + namespace + " found in DB"));
                    }
                    return null;
                }
                resultMetaObjects = (List<MetaInfo.MetaObject>)resultList;
                final List<MetaInfo.MetaObject> toBeReturned = new ArrayList<MetaInfo.MetaObject>();
                for (final MetaInfo.MetaObject metaObject : resultMetaObjects) {
                    if (metaObject.getType().ordinal() == EntityType.FLOW.ordinal()) {
                        final boolean isApp = this.checkIfFlowIsApplication(metaObject);
                        if (isApp) {
                            toBeReturned.add(this.convertFlowToApplication(metaObject));
                        }
                        else {
                            toBeReturned.add(metaObject);
                        }
                    }
                    else {
                        toBeReturned.add(metaObject);
                    }
                }
                return new HashSet<MetaInfo.MetaObject>(toBeReturned);
            }
        }
        catch (Exception e) {
            if (e instanceof MetaDataDBOpsException) {
                throw new MetaDataDBOpsException(e.getMessage());
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
            return null;
        }
    }
    
    private Set<MetaInfo.MetaObject> loadByEntityTypeInNamespace(final String namespace, final EntityType eType) throws MetaDataDBOpsException {
        try {
            MDConstants.checkNullParams("Can't load MetaObject with NULL values for either of, Entity Type: " + eType + ", Namespace: " + namespace, eType, namespace);
            if (eType.isStoreable()) {
                List<MetaInfo.MetaObject> resultMetaObjects = null;
                if (MetaDataDBOps.logger.isDebugEnabled()) {
                    MetaDataDBOps.logger.debug((Object)("Load by Namespace : " + namespace));
                }
                final EntityManager manager = getManager();
                synchronized (manager) {
                    final String query = "select w from MetaInfo$MetaObject w where w.nsName = :nsName AND w.type = :type";
                    final Query q = manager.createQuery(query);
                    q.setParameter("nsName", (Object)namespace);
                    if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
                        q.setParameter("type", (Object)EntityType.FLOW);
                    }
                    else {
                        q.setParameter("type", (Object)eType);
                    }
                    final List<?> resultList = (List<?>)q.getResultList();
                    if (resultList.size() == 0) {
                        if (MetaDataDBOps.logger.isDebugEnabled()) {
                            MetaDataDBOps.logger.debug((Object)("No Object of type : " + namespace + " found in DB"));
                        }
                        return null;
                    }
                    resultMetaObjects = (List<MetaInfo.MetaObject>)resultList;
                    final List<MetaInfo.MetaObject> toBeReturned = new ArrayList<MetaInfo.MetaObject>();
                    if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
                        for (final MetaInfo.MetaObject metaObject : resultMetaObjects) {
                            final boolean isApp = this.checkIfFlowIsApplication(metaObject);
                            if (isApp) {
                                toBeReturned.add(this.convertFlowToApplication(metaObject));
                            }
                        }
                        resultMetaObjects = toBeReturned;
                    }
                    else if (eType.ordinal() == EntityType.FLOW.ordinal()) {
                        for (final MetaInfo.MetaObject metaObject : resultMetaObjects) {
                            final boolean isApp = this.checkIfFlowIsApplication(metaObject);
                            if (!isApp) {
                                toBeReturned.add(metaObject);
                            }
                        }
                        resultMetaObjects = toBeReturned;
                    }
                    return new HashSet<MetaInfo.MetaObject>(resultMetaObjects);
                }
            }
            return null;
        }
        catch (Exception e) {
            if (e instanceof MetaDataDBOpsException) {
                throw new MetaDataDBOpsException(e.getMessage());
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
            return null;
        }
    }
    
    public Integer loadMaxClassId() {
        final EntityManager manager = getManager();
        synchronized (manager) {
            final String query = "Select max(w.classId) from MetaInfo$Type w";
            final Query q = manager.createQuery(query);
            final Integer result = (Integer)q.getSingleResult();
            return result;
        }
    }
    
    private Integer removeByUUID(final UUID uuid) throws MetaDataDBOpsException {
        EntityTransaction txn = null;
        Integer deleteCount = 0;
        try {
            MDConstants.checkNullParams("Can't remove Meta Object from Database with a NULL value UUID", uuid);
            final String delQuery = "DELETE from MetaInfo$MetaObject w where w.uuid = :uuid";
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)("Deleted objects using query " + delQuery + " uuid = " + uuid));
            }
            final EntityManager m = getManager();
            synchronized (m) {
                m.clear();
                txn = m.getTransaction();
                txn.begin();
                final Query dq = m.createQuery(delQuery);
                dq.setParameter("uuid", (Object)uuid);
                deleteCount = dq.executeUpdate();
                m.flush();
                txn.commit();
            }
        }
        catch (Exception e) {
            if (txn != null && txn.isActive()) {
                txn.rollback();
            }
            MetaDataDBOps.logger.error((Object)("Problem deleting object with key " + uuid + " from Database \nREASON: \n" + e.getMessage()));
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error ", (Throwable)e);
                System.exit(1);
            }
        }
        return deleteCount;
    }
    
    private Integer removeByName(final EntityType eType, final String namespace, final String name, final Integer version) throws MetaDataDBOpsException {
        EntityTransaction txn = null;
        Integer deleteCount = 0;
        String uri = null;
        try {
            MDConstants.checkNullParams("Can't remove Meta Object from Database with a NULL for either of Entity Type, Namespace, Object Name, Version", eType, namespace, name, version);
            uri = MDConstants.makeURLWithVersion(eType, namespace, name, version);
            final String delQuery = "DELETE from MetaInfo$MetaObject w where w.uri = :uri";
            if (MetaDataDBOps.logger.isDebugEnabled()) {
                MetaDataDBOps.logger.debug((Object)("Deleted objects using query " + delQuery + " uri = " + uri));
            }
            final EntityManager m = getManager();
            synchronized (m) {
                m.clear();
                txn = m.getTransaction();
                txn.begin();
                final Query dq = m.createQuery(delQuery);
                dq.setParameter("uri", (Object)uri);
                deleteCount = dq.executeUpdate();
                m.flush();
                txn.commit();
            }
        }
        catch (Exception e) {
            if (txn != null && txn.isActive()) {
                txn.rollback();
            }
            MetaDataDBOps.logger.error((Object)("Problem deleting object with key " + uri + " from store"));
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
        }
        return deleteCount;
    }
    
    public Integer clear() {
        int n = 0;
        EntityTransaction txn = null;
        EntityManager em = null;
        try {
            em = getManager();
            synchronized (em) {
                final String query = "Delete from MetaInfo$MetaObject w";
                if (MetaDataDBOps.logger.isInfoEnabled()) {
                    MetaDataDBOps.logger.info((Object)("Deleting all rows from DB using " + query));
                }
                txn = em.getTransaction();
                txn.begin();
                final TypedQuery<Object[]> q = (TypedQuery<Object[]>)em.createQuery(query, (Class)Object[].class);
                n = q.executeUpdate();
                MetaDataDBOps.logger.warn((Object)("total deleted : " + n));
                txn.commit();
            }
            return n;
        }
        catch (Exception e) {
            MetaDataDBOps.logger.error((Object)"Problem deleting all from DB", (Throwable)e);
            if (txn != null && txn.isActive()) {
                txn.rollback();
            }
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    MetaDataDBOps.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                    System.exit(1);
                }
            }
            else if (e instanceof PersistenceException) {
                MetaDataDBOps.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
                System.exit(1);
            }
            return n;
        }
    }
    
    public void shutdown() {
        final EntityManager m = getManager();
        m.close();
    }
    
    static {
        logger = Logger.getLogger((Class)MetaDataDBOps.class);
        MetaDataDBOps.INSTANCE = new MetaDataDBOps();
        MetaDataDBOps.DBLocation = null;
        MetaDataDBOps.DBName = null;
        MetaDataDBOps.DBUname = null;
        MetaDataDBOps.DBPassword = null;
    }
}
