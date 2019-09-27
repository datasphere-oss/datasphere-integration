package com.datasphere.Tables;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.WASecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.source.lib.meta.DatabaseColumn;

public class UUIDTable extends Table
{
    public UUIDTable(final UUID typeUUID, final String alias) throws AdapterException {
        this.init(typeUUID, alias);
    }
    
    private void init(final UUID typeUUID, final String alias) throws AdapterException {
        try {
            final MetaInfo.Type type = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(typeUUID, WASecurityManager.TOKEN);
            final List<String> keyFields = (List<String>)type.keyFields;
            final Map<String, String> colMap = (Map<String, String>)type.fields;
            final Map<String, Boolean> keyMap = new HashMap<String, Boolean>();
            for (int itr = 0; itr < keyFields.size(); ++itr) {
                keyMap.put((String)keyFields.get(itr), true);
            }
            final Class<?> srcTable = ClassLoader.getSystemClassLoader().loadClass(type.className);
            final Field[] fields = srcTable.getDeclaredFields();
            String colName = "";
            Boolean isKey = false;
            final ArrayList<DatabaseColumn> cols = new ArrayList<DatabaseColumn>();
            for (int itr2 = 0; itr2 < fields.length; ++itr2) {
                colName = fields[itr2].getName();
                if (colMap.containsKey(colName)) {
                    isKey = keyMap.containsKey(colName);
                    cols.add(new DBWriterColumn(fields[itr2].getName(), fields[itr2].getType(), itr2, isKey));
                }
            }
            this.columns = new DatabaseColumn[cols.size()];
            for (int itr2 = 0; itr2 < cols.size(); ++itr2) {
                this.columns[itr2] = cols.get(itr2);
            }
            this.setName(alias);
        }
        catch (MetaDataRepositoryException e) {
            throw new AdapterException("Failed to get complete details of Source " + e.getMessage(), (Throwable)e);
        }
        catch (ClassNotFoundException e2) {
            throw new AdapterException("Failed to get complete details of Class " + e2.getMessage(), (Throwable)e2);
        }
    }
}
