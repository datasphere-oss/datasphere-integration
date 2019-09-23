package com.datasphere.persistence;

import org.apache.log4j.*;

import com.datasphere.gen.*;
import com.datasphere.runtime.*;
import java.io.*;
import java.util.*;
import javax.persistence.*;
import org.apache.commons.lang.*;

public class DefaultRuntimeJPAPersistenceLayerImpl extends DefaultJPAPersistenceLayerImpl
{
    private static Logger logger;
    
    public DefaultRuntimeJPAPersistenceLayerImpl() {
    }
    
    public DefaultRuntimeJPAPersistenceLayerImpl(final String persistenceUnit, final Map<String, Object> properties) {
        super(persistenceUnit, properties);
    }
    
    protected void createPersistenceFile() {
        String pxml = "";
        try {
            final String persistence_file_name = "persistence_sample.xml";
            final InputStream in = ClassLoader.getSystemResourceAsStream(persistence_file_name);
            final char[] buffer = new char[1024];
            final StringBuilder sb = new StringBuilder();
            final Reader reader = new InputStreamReader(in);
            int nn = 0;
            while (true) {
                nn = reader.read(buffer);
                if (nn < 0) {
                    break;
                }
                sb.append(buffer, 0, nn);
            }
            pxml = sb.toString();
            pxml = pxml.replace("$PUNAME", this.persistenceUnitName);
            if (DefaultRuntimeJPAPersistenceLayerImpl.logger.isDebugEnabled()) {
                DefaultRuntimeJPAPersistenceLayerImpl.logger.debug((Object)("persistence xml for store : " + this.persistenceUnitName + "\n" + pxml));
            }
        }
        catch (IOException ex) {
            DefaultRuntimeJPAPersistenceLayerImpl.logger.error((Object)("error creating custom persistence.xml file for :" + this.persistenceUnitName + ex));
        }
        synchronized (HStore.class) {
            final File metadir = new File(NodeStartUp.getPlatformHome() + "/conf/META-INF");
            if (!metadir.exists()) {
                metadir.mkdir();
            }
        }
        final String persistence_file_name = NodeStartUp.getPlatformHome() + "/conf/META-INF/persistence_" + this.persistenceUnitName + ".xml";
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File(persistence_file_name)));
            writer.write(pxml);
        }
        catch (Exception e) {
            DefaultRuntimeJPAPersistenceLayerImpl.logger.error((Object)"error writing persistence file ", (Throwable)e);
        }
        finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            }
            catch (IOException ex2) {}
        }
        if (DefaultRuntimeJPAPersistenceLayerImpl.logger.isDebugEnabled()) {
            DefaultRuntimeJPAPersistenceLayerImpl.logger.debug((Object)("successfully created persistence xml file for persistence unit :" + this.persistenceUnitName));
        }
    }
    
    protected void createORMXMLMappingFile() {
        final String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/eclipselink-orm-" + this.persistenceUnitName + ".xml";
        final String xml = RTMappingGenerator.getMappings(this.persistenceUnitName);
        if (DefaultRuntimeJPAPersistenceLayerImpl.logger.isDebugEnabled()) {
            DefaultRuntimeJPAPersistenceLayerImpl.logger.debug((Object)("xml mapping for event :\n" + xml));
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File(mapping_file_name)));
            writer.write(xml);
        }
        catch (Exception e) {
            DefaultRuntimeJPAPersistenceLayerImpl.logger.error((Object)"error writing orm mapping. ", (Throwable)e);
        }
        finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            }
            catch (IOException ex) {}
        }
    }
    
    @Override
    public void init() {
        if (this.factory == null || !this.factory.isOpen()) {
            this.createPersistenceFile();
            this.createORMXMLMappingFile();
            this.props.put("eclipselink.persistencexml", "META-INF/persistence_" + this.persistenceUnitName + ".xml");
            final HMetadataSource mms = new HMetadataSource();
            mms.setStoreName(this.persistenceUnitName);
            this.props.put("eclipselink.metadata-source", mms);
            this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName, (Map)this.props);
            final Map<String, Object> origMap = (Map<String, Object>)this.factory.getProperties();
            for (final String key : origMap.keySet()) {
                if (!this.props.containsKey(key)) {
                    this.props.put(key.toUpperCase(), origMap.get(key));
                }
            }
            final EntityManager em = this.getEntityManager();
            if (em != null) {
                em.close();
            }
        }
    }
    
    @Override
    public int delete(final Object object) {
        throw new NotImplementedException();
    }
    
    static {
        DefaultRuntimeJPAPersistenceLayerImpl.logger = Logger.getLogger((Class)DefaultRuntimeJPAPersistenceLayerImpl.class);
    }
}
