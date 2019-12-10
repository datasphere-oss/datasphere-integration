package com.datasphere.source.lib.formatter;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.source.lib.utils.FieldModifier;

public abstract class CDCArrayFormatter extends FieldModifier
{
    private Logger logger;
    private HashMap<UUID, Field[]> typeUUIDCache;
    private Field field;
    
    public CDCArrayFormatter(final Field field) {
        this.logger = Logger.getLogger((Class)CDCArrayFormatter.class);
        this.typeUUIDCache = new HashMap<UUID, Field[]>();
        this.field = field;
    }
    
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) throws Exception {
        final Object object = this.field.get(event);
        if (object != null) {
            final HDEvent hdEvent = (HDEvent)event;
            Field[] fieldsOfThisTable = null;
            if (hdEvent.typeUUID != null) {
                if (this.typeUUIDCache.containsKey(hdEvent.typeUUID)) {
                    fieldsOfThisTable = this.typeUUIDCache.get(hdEvent.typeUUID);
                }
                else {
                    try {
                        final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(hdEvent.typeUUID, HDSecurityManager.TOKEN);
                        final Class<?> typeClass = ClassLoader.getSystemClassLoader().loadClass(dataType.className);
                        fieldsOfThisTable = typeClass.getDeclaredFields();
                        this.typeUUIDCache.put(hdEvent.typeUUID, fieldsOfThisTable);
                    }
                    catch (MetaDataRepositoryException | ClassNotFoundException e) {
                        this.logger.warn((Object)("Unable to fetch the type for table " + hdEvent.metadata.get("TableName") + e));
                    }
                }
            }
            return this.formatCDCArray(hdEvent, (Object[])object, fieldsOfThisTable);
        }
        return null;
    }
    
    public abstract String formatCDCArray(final HDEvent p0, final Object[] p1, final Field[] p2);
}
