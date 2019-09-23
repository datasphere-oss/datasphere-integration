package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.fileMetaExtension.*;
import org.eclipse.persistence.exceptions.*;
import java.sql.*;
import javax.persistence.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.text.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import com.datasphere.runtime.meta.cdc.filters.*;

public class FileMetadataDBOperations
{
    private static final Logger logger;
    private String PERSISTENCE_UNIT_NAME;
    private String LOGGING_LEVEL;
    protected String DBLocation;
    protected String DBName;
    protected String DBUname;
    protected String DBPassword;
    private static volatile EntityManager entityManager;
    private static boolean allowDbOperations;
    
    public FileMetadataDBOperations() {
        this.PERSISTENCE_UNIT_NAME = "FileMetadataStore";
        this.LOGGING_LEVEL = "SEVERE";
        this.DBLocation = null;
        this.DBName = null;
        this.DBUname = null;
        this.DBPassword = null;
    }
    
    public FileMetadataDBOperations(final String DBLocation, final String DBName, final String DBUname, final String DBPassword) {
        this.PERSISTENCE_UNIT_NAME = "FileMetadataStore";
        this.LOGGING_LEVEL = "SEVERE";
        this.DBLocation = null;
        this.DBName = null;
        this.DBUname = null;
        this.DBPassword = null;
        this.DBLocation = DBLocation;
        this.DBName = DBName;
        this.DBUname = DBUname;
        this.DBPassword = DBPassword;
        if (this.allowFileMetadataDbOperations()) {
            FileMetadataDBOperations.allowDbOperations = true;
        }
    }
    
    public static FileMetadataDBOperations getFileMetadataDBInstance(final Map<String, MetaInfo.Initializer> map) {
        final String clusterName = HazelcastSingleton.getClusterName();
        if (clusterName != null) {
            final MetaInfo.Initializer initializer = map.get(clusterName);
            final String DBName = initializer.getMetaDataRepositoryDBname();
            final String DBLocation = initializer.getMetaDataRepositoryLocation();
            final String DBUname = initializer.getMetaDataRepositoryUname();
            final String DBPassword = initializer.getMetaDataRepositoryPass();
            return new FileMetadataDBOperations(DBLocation, DBName, DBUname, DBPassword);
        }
        return null;
    }
    
    public EntityManager getManager() {
        if (FileMetadataDBOperations.entityManager == null) {
            synchronized (FileMetadataDBOperations.class) {
                if (FileMetadataDBOperations.entityManager == null) {
                    FileMetadataDBOperations.entityManager = this.initManager();
                }
            }
        }
        return FileMetadataDBOperations.entityManager;
    }
    
    private EntityManager initManager() {
        try {
            final Map<String, Object> properties = new HashMap<String, Object>();
            final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
            properties.put("javax.persistence.jdbc.user", this.DBUname);
            properties.put("javax.persistence.jdbc.password", this.DBPassword);
            properties.put("javax.persistence.jdbc.url", metaDataDbProvider.getJDBCURL(this.DBLocation, this.DBName, this.DBUname, this.DBPassword));
            properties.put("javax.persistence.jdbc.driver", metaDataDbProvider.getJDBCDriver());
            properties.put("eclipselink.logging.level", this.LOGGING_LEVEL);
            properties.put("eclipselink.weaving", "STATIC");
            final EntityManagerFactory m_factory = Persistence.createEntityManagerFactory(this.PERSISTENCE_UNIT_NAME, (Map)properties);
            return m_factory.createEntityManager();
        }
        catch (Exception e) {
            FileMetadataDBOperations.logger.error((Object)e, (Throwable)e);
            if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)("Shutting down server it couldn't connect to database with error: " + e.getMessage()));
            }
            throw e;
        }
    }
    
    public boolean store(final FileMetadataExtension mObject) {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return false;
        }
        EntityTransaction txn = null;
        final EntityManager m = this.getManager();
        try {
            synchronized (m) {
                m.clear();
                txn = m.getTransaction();
                txn.begin();
                m.merge((Object)mObject);
                m.flush();
                txn.commit();
            }
        }
        catch (Exception e) {
            FileMetadataDBOperations.logger.error((Object)("Problem storing file metadata extension object for file name : " + mObject.getFileName() + "in directory named : " + mObject.getDirectoryName()), (Throwable)e);
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error ", (Throwable)e);
            }
            else {
                FileMetadataDBOperations.logger.error((Object)"Error while writing to metadata database", (Throwable)e);
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
            }
            throw e;
        }
        return true;
    }
    
    public boolean store(final Collection<FileMetadataExtension> fileMetadataExtensions) {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return false;
        }
        for (final FileMetadataExtension extension : fileMetadataExtensions) {
            this.store(extension);
        }
        return true;
    }
    
    public Set<FileMetadataExtension> getAllFileMetadata() {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return null;
        }
        Set<FileMetadataExtension> resultMetaObjects = null;
        try {
            if (FileMetadataDBOperations.logger.isDebugEnabled()) {
                FileMetadataDBOperations.logger.debug((Object)"Load All");
            }
            final EntityManager manager = this.getManager();
            synchronized (manager) {
                final String query = "Select w from FileMetadata w ";
                final Query q = manager.createQuery(query);
                final List<?> resultList = (List<?>)q.getResultList();
                if (resultList != null) {
                    if (FileMetadataDBOperations.logger.isDebugEnabled()) {
                        FileMetadataDBOperations.logger.debug((Object)("Found " + resultList.size() + " objects in Database."));
                    }
                    resultMetaObjects = new LinkedHashSet<FileMetadataExtension>();
                    for (final Object o : resultList) {
                        resultMetaObjects.add((FileMetadataExtension)o);
                    }
                }
                else if (FileMetadataDBOperations.logger.isDebugEnabled()) {
                    FileMetadataDBOperations.logger.debug((Object)"No Objects found in Database.");
                }
            }
        }
        catch (Exception e) {
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
            }
        }
        return resultMetaObjects;
    }
    
    public Set<FileMetadataExtension> query(final FileTrailFilter filter) {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return null;
        }
        List<FileMetadataExtension> resultList = null;
        final String componentName = filter.getComponentName().toLowerCase();
        final String startTime = filter.getStartTime();
        String endTime = filter.getEndTime();
        String status = filter.getStatus();
        final Integer limit = filter.getLimit();
        Long startTimeStamp = null;
        Long endTimeStamp = null;
        final boolean isDescending = filter.isDescending();
        final UUID uuid = filter.getUuid();
        if (status != null) {
            status = status.toLowerCase();
        }
        final EntityManager m = this.getManager();
        final StringBuilder sb = new StringBuilder("Select fme from com.datasphere.runtime.fileMetaExtension.FileMetadataExtension fme where LOWER(fme.parentComponent)= :componentName");
        if (startTime != null) {
            startTimeStamp = this.getTimeStampFromDate(startTime);
            sb.append(" and fme.externalFileCreationTime>= :startTimeStamp");
        }
        if (endTime != null) {
            endTime = this.moveToNextDay(endTime);
            endTimeStamp = this.getTimeStampFromDate(endTime);
            sb.append(" and fme.externalFileCreationTime<= :endTimeStamp");
        }
        if (uuid != null) {
            sb.append(" and fme.parentComponentUUID= :uuid");
        }
        if (status != null) {
            sb.append(" and LOWER(fme.status)= :status");
        }
        if (isDescending) {
            sb.append(" order by fme.externalFileCreationTime DESC");
        }
        else {
            sb.append(" order by fme.externalFileCreationTime ASC");
        }
        try {
            synchronized (m) {
                m.clear();
                final String queryStr = new String(sb);
                final Query q = m.createQuery(queryStr);
                q.setParameter("componentName", (Object)componentName);
                if (startTime != null) {
                    q.setParameter("startTimeStamp", (Object)startTimeStamp);
                }
                if (endTime != null) {
                    q.setParameter("endTimeStamp", (Object)endTimeStamp);
                }
                if (uuid != null) {
                    q.setParameter("uuid", (Object)uuid);
                }
                if (status != null) {
                    q.setParameter("status", (Object)status);
                }
                resultList = (List<FileMetadataExtension>)q.setMaxResults((int)limit).getResultList();
            }
        }
        catch (Exception e) {
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
            }
        }
        if (resultList != null) {
            return new LinkedHashSet<FileMetadataExtension>(resultList);
        }
        return null;
    }
    
    private long getTimeStampFromDate(final String str) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = dateFormat.parse(str);
        }
        catch (ParseException e) {
            FileMetadataDBOperations.logger.error((Object)("Error while converting date to timestamp : " + e.getMessage()));
        }
        return date.getTime();
    }
    
    private String moveToNextDay(final String endTime) {
        final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            final Date date = format.parse(endTime);
            final Date datePlusOne = new Date(date.getTime() + TimeUnit.DAYS.toMillis(1L));
            return format.format(datePlusOne);
        }
        catch (ParseException e) {
            FileMetadataDBOperations.logger.error((Object)("Date parsing failed while tracking lineage : " + e.getMessage()));
            return null;
        }
    }
    
    public int getTotalRowsForTheParentComponent(String componentName, final UUID uuid) {
        componentName = componentName.toLowerCase();
        final EntityManager m = this.getManager();
        final StringBuilder sb = new StringBuilder("Select count(fme.parentComponent) from com.datasphere.runtime.fileMetaExtension.FileMetadataExtension fme where LOWER(fme.parentComponent)= :componentName");
        if (uuid != null) {
            sb.append(" and fme.parentComponentUUID= :uuid");
        }
        try {
            synchronized (m) {
                final String queryStr = new String(sb);
                final Query q = m.createQuery(queryStr);
                q.setParameter("componentName", (Object)componentName);
                if (uuid != null) {
                    q.setParameter("uuid", (Object)uuid);
                }
                final long count = (long)q.getSingleResult();
                return Math.toIntExact(count);
            }
        }
        catch (Exception e) {
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
            }
            return -1;
        }
    }
    
    public Set<FileMetadataExtension> query(final FileMetadataFilter filter) {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return null;
        }
        List<FileMetadataExtension> resultList = null;
        final String parentComponent = filter.getParentComponent().toLowerCase();
        final String directoryName = filter.getDirectoryName().toLowerCase();
        final UUID componentUUID = filter.getComponentUUID();
        final String distributionID = filter.getDistributionID();
        final EntityManager m = this.getManager();
        try {
            synchronized (m) {
                final String queryStr = "Select fme from com.datasphere.runtime.fileMetaExtension.FileMetadataExtension fme where LOWER(fme.parentComponent)= :parentComponent and LOWER(fme.directoryName)= :directoryName and fme.parentComponentUUID= :componentUUID and fme.distributionID= :distributionID order by fme.creationTimeStamp desc";
                final Query q = m.createQuery(queryStr);
                q.setParameter("parentComponent", (Object)parentComponent);
                q.setParameter("directoryName", (Object)directoryName);
                q.setParameter("componentUUID", (Object)componentUUID);
                q.setParameter("distributionID", (Object)distributionID);
                resultList = (List<FileMetadataExtension>)q.getResultList();
            }
        }
        catch (Exception e) {
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
            }
        }
        if (resultList != null) {
            return new LinkedHashSet<FileMetadataExtension>(resultList);
        }
        return null;
    }
    
    public boolean removeFileMetadata(final String parentComponent) {
        if (!FileMetadataDBOperations.allowDbOperations) {
            return false;
        }
        final EntityManager m = this.getManager();
        EntityTransaction txn = null;
        Integer deleteCount = 0;
        try {
            final String deleteStr = "Delete from com.datasphere.runtime.fileMetaExtension.FileMetadataExtension fme where fme.parentComponent= :parentComponent";
            synchronized (m) {
                m.clear();
                txn = m.getTransaction();
                txn.begin();
                final Query dq = m.createQuery(deleteStr);
                dq.setParameter("parentComponent", (Object)parentComponent);
                deleteCount = dq.executeUpdate();
                m.flush();
                txn.commit();
            }
        }
        catch (Exception e) {
            if (txn != null && txn.isActive()) {
                txn.rollback();
            }
            FileMetadataDBOperations.logger.error((Object)("Problem deleting object with parent component " + parentComponent + " from datastore"));
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    FileMetadataDBOperations.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode()), (Throwable)e);
                }
            }
            else if (e instanceof PersistenceException) {
                FileMetadataDBOperations.logger.error((Object)"Shutting down server becasue it could not connect to dataabase with error", (Throwable)e);
            }
        }
        return deleteCount > 0;
    }
    
    private boolean allowFileMetadataDbOperations() {
        final String flag = System.getProperty("com.datasphere.config.trackFileLineageMetadata", "false");
        return flag.equalsIgnoreCase("true");
    }
    
    static {
        logger = Logger.getLogger((Class)FileMetadataDBOperations.class);
    }
}
