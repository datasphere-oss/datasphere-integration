package com.datasphere.persistence;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

import com.datasphere.gen.RTMappingGenerator;
import com.datasphere.intf.PersistenceLayer;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.NodeStartUp;
import com.datasphere.hd.HD;
import com.datasphere.hd.HDKey;

public class HibernatePersistenceLayerImpl implements PersistenceLayer
{
    private static Logger logger;
    private Map<String, Object> props;
    private String storeName;
    private boolean metaDataRefreshed;
    private Configuration configure;
    private SessionFactory factory;
    private Session session;
    private static final String MXSQL_INSTR_PROP = "mxsql.tablecreation.instructions";
    
    public HibernatePersistenceLayerImpl(final String persistenceUnitName) {
        this(persistenceUnitName, null);
    }
    
    public HibernatePersistenceLayerImpl(final String pu, final Map<String, Object> propss) {
        this.props = new HashMap<String, Object>();
        this.storeName = "default";
        this.metaDataRefreshed = false;
        this.configure = null;
        this.factory = null;
        this.session = null;
        if (this.props != null) {
            this.props.putAll(propss);
            final Set<String> keys = propss.keySet();
            for (final String k : keys) {
                if (propss.get(k) == null) {
                    this.props.remove(k);
                }
            }
        }
        if (HibernatePersistenceLayerImpl.logger.isDebugEnabled()) {
            HibernatePersistenceLayerImpl.logger.debug((Object)("properties : " + this.props.toString()));
        }
    }
    
    @Override
    public void init() {
        this.init(null);
    }
    
    @Override
    public void init(final String storeName) {
        final String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/" + storeName + ".hbm.xml";
        final File file = new File(mapping_file_name);
        if (file.exists()) {
            (this.configure = new Configuration()).configure("hibernate.cfg.xml");
            this.configure.addFile(mapping_file_name);
            final Properties properties = new Properties();
            properties.putAll(this.props);
            this.configure.setProperties(properties);
            final ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().applySettings((Map)this.configure.getProperties()).buildServiceRegistry();
            this.factory = this.configure.buildSessionFactory(serviceRegistry);
        }
    }
    
    @Override
    public int delete(final Object object) {
        throw new NotImplementedException();
    }
    
    @Override
    public Range[] persist(final Object object) {
        final Range[] rangeArray = { null };
        if (object == null) {
            HibernatePersistenceLayerImpl.logger.warn((Object)"Got null object to persist so, simply skip everything from here");
            return rangeArray;
        }
        if (!this.metaDataRefreshed && object instanceof HD) {
            final String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/" + this.storeName + ".hbm.xml";
            final String xml = RTMappingGenerator.getMappings(((HD)object).getInternalHDStoreName());
            if (HibernatePersistenceLayerImpl.logger.isDebugEnabled()) {
                HibernatePersistenceLayerImpl.logger.debug((Object)("hibernate xml mapping for hdstore :\n" + xml));
            }
            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(new File(mapping_file_name)));
                writer.write(xml);
            }
            catch (Exception e) {
                HibernatePersistenceLayerImpl.logger.error((Object)"error writing orm mapping. ", (Throwable)e);
            }
            finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                }
                catch (IOException ex) {}
            }
            this.init(this.storeName);
            this.createTable();
            this.metaDataRefreshed = true;
        }
        synchronized (object) {
            if (HibernatePersistenceLayerImpl.logger.isTraceEnabled()) {
                HibernatePersistenceLayerImpl.logger.trace((Object)("persisting object of type : " + object.getClass().getCanonicalName() + "\nobject=" + object.toString()));
            }
            if (this.session == null || !this.session.isOpen()) {
                this.session = this.factory.openSession();
            }
            final Transaction tx = this.session.beginTransaction();
            this.session.persist(object);
            tx.commit();
            if (HibernatePersistenceLayerImpl.logger.isTraceEnabled()) {
                HibernatePersistenceLayerImpl.logger.trace((Object)("done persisting object of type : " + object.getClass().getCanonicalName() + "\nobject=" + object.toString()));
            }
        }
        int numWritten = 1;
        if (object instanceof List) {
            numWritten = ((List)object).size();
        }
        (rangeArray[0] = new Range(0, numWritten)).setSuccessful(true);
        return rangeArray;
    }
    
    private void createTable() {
        final String dialectClassName = (String)this.props.get("hibernate.dialect");
        String inst = (String)this.props.get("mxsql.tablecreation.instructions");
        final String hbm2ddl = (String)this.props.get("hibernate.hbm2ddl.auto");
        Connection conn = null;
        if (hbm2ddl != null && hbm2ddl.equalsIgnoreCase("none")) {
            if (dialectClassName == null) {
                return;
            }
            try {
                final Class dialectClazz = Class.forName(dialectClassName, false, ClassLoader.getSystemClassLoader());
                final Object dialect = dialectClazz.newInstance();
                dialectClazz.cast(dialect);
                final String[] qrs = this.configure.generateSchemaCreationScript((Dialect)dialect);
                if (inst == null || inst.trim().length() == 0) {
                    inst = "STORE BY PRIMARY KEY ATTRIBUTE BLOCKSIZE 4096";
                }
                final String sql = qrs[0] + " " + inst + " ;";
                if (HibernatePersistenceLayerImpl.logger.isDebugEnabled()) {
                    HibernatePersistenceLayerImpl.logger.debug((Object)("DDL to create table : \n" + sql));
                }
                final String driver = (String)this.props.get("hibernate.connection.driver_class");
                final String url = (String)this.props.get("hibernate.connection.url");
                final String usr = (String)this.props.get("hibernate.connection.username");
                final String pwd = (String)this.props.get("hibernate.connection.password");
                if (HibernatePersistenceLayerImpl.logger.isDebugEnabled()) {
                    HibernatePersistenceLayerImpl.logger.debug((Object)("driver=" + driver + ", url=" + url + ", usr=" + usr + ",pwd=" + pwd));
                }
                Class.forName(driver, true, ClassLoader.getSystemClassLoader());
                conn = DriverManager.getConnection(url, usr, pwd);
                final Statement stmt = conn.createStatement();
                stmt.executeUpdate(sql);
            }
            catch (SQLException ex2) {}
            catch (ClassNotFoundException ex3) {}
            catch (InstantiationException ex4) {}
            catch (IllegalAccessException ex) {
                HibernatePersistenceLayerImpl.logger.error((Object)("error in creating table :" + ex));
            }
            finally {
                try {
                    conn.close();
                }
                catch (SQLException ex5) {}
            }
        }
        else if (HibernatePersistenceLayerImpl.logger.isInfoEnabled()) {
            HibernatePersistenceLayerImpl.logger.info((Object)"Letting Hibernate to create table.");
        }
    }
    
    @Override
    public void merge(final Object object) {
    }
    
    @Override
    public void setStoreName(final String storeName, final String tableName) {
        this.storeName = storeName;
    }
    
    @Override
    public Object get(final Class<?> objectClass, final Object objectId) {
        return null;
    }
    
    @Override
    public List<?> runQuery(final String query, final Map<String, Object> params, final Integer maxResults) {
        return null;
    }
    
    @Override
    public Object runNativeQuery(final String query) {
        return null;
    }
    
    @Override
    public int executeUpdate(final String query, final Map<String, Object> params) {
        return 0;
    }
    
    @Override
    public <T extends HD> List<T> getResults(final Class<T> objectClass, final Map<String, Object> filter, final Set<HDKey> excludeKeys) {
        final String clazzName = objectClass.getCanonicalName();
        List<T> results = null;
        final long T1 = System.currentTimeMillis();
        String qry = "FROM " + clazzName + " AS W ";
        if (excludeKeys != null && excludeKeys.size() > 0) {
            qry += " WHERE W.MapKeyString NOT IN ( ";
            for (final HDKey k : excludeKeys) {
                qry = qry + "'" + k.getHDKeyStr() + "',";
            }
            qry = qry.substring(0, qry.length() - 1);
            qry += ")";
        }
        final long T2 = System.currentTimeMillis();
        if (HibernatePersistenceLayerImpl.logger.isDebugEnabled()) {
            HibernatePersistenceLayerImpl.logger.debug((Object)("SQL creation taken (milli sec) : " + (T2 - T1)));
        }
        try {
            if (this.session == null || !this.session.isOpen()) {
                this.session = this.factory.openSession();
            }
            final Query query = this.session.createQuery(qry);
            results = (List<T>)query.list();
            return results;
        }
        catch (Exception ex) {
            HibernatePersistenceLayerImpl.logger.error((Object)("error querying hd table :" + clazzName + ex));
        }
        return null;
    }
    
    @Override
    public <T extends HD> List<T> getResults(final Class<T> objectClass, final String hdKey, final Map<String, Object> filter) {
        final String clazzName = objectClass.getCanonicalName();
        List<T> results = null;
        final String qry = "FROM " + clazzName + "as W where W.MapKeyString = '" + hdKey + "'";
        try {
            if (this.session == null || !this.session.isOpen()) {
                this.session = this.factory.openSession();
            }
            final Query query = this.session.createQuery(qry);
            results = (List<T>)query.list();
            return results;
        }
        catch (Exception ex) {
            HibernatePersistenceLayerImpl.logger.error((Object)("error querying hd table :" + clazzName + ex));
        }
        return null;
    }
    
    @Override
    public void close() {
        if (this.session != null && this.session.isOpen()) {
            this.session.flush();
            this.session.close();
            this.factory.close();
        }
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for HibernatePersistenceLayerImpl");
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for HibernatePersistenceLayerImpl");
    }
    
    @Override
    public HStore getHDStore() {
        return null;
    }
    
    @Override
    public void setHDStore(final HStore ws) {
    }
    
    static {
        HibernatePersistenceLayerImpl.logger = Logger.getLogger((Class)HibernatePersistenceLayerImpl.class);
    }
}
