package com.datasphere.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.uuid.UUID;

public class UUIDTable extends Table
{
    public UUIDTable(final UUID typeUUID, final String alias) {
        this.init(typeUUID, alias);
    }
    
    private void init(final UUID typeUUID, final String alias) {
        try {
            final MetaInfo.Type type = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(typeUUID, HDSecurityManager.TOKEN);
            final List<String> keyFields = (List<String>)type.keyFields;
            final Map<String, String> colMap = (Map<String, String>)type.fields;
            final Map<String, Boolean> keyMap = new HashMap<String, Boolean>();
            for (int itr = 0; itr < keyFields.size(); ++itr) {
                keyMap.put(keyFields.get(itr), true);
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
            e.printStackTrace();
        }
        catch (ClassNotFoundException e2) {
            e2.printStackTrace();
        }
    }
}

