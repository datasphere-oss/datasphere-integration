package com.datasphere.proc;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.event.Event;
import com.datasphere.security.Password;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

public class DatabaseWriterProc
{
    private static Logger logger;
    
    public static void main(final String[] args) throws Exception {
        final Map<String, Object> prop = new HashMap<String, Object>();
        prop.put("Username", "qatest");
        prop.put("Password", new Password("w@ct10n"));
        prop.put("ConnectionURL", "jdbc:sqlserver://10.1.186.103:1433;databaseName=justin");
        prop.put("Tables", "WA.SAMPLES_JUSTIN.TEST_SQLMX,dbo.TEST_SQLMX");
        prop.put("BatchPolicy", "EventCount:1,Interval:0");
        prop.put("CommitPolicy", "");
        final DatabaseWriterNew writer = new DatabaseWriterNew();
        try {
            final Map<String, Object> localCopyOfProperty = new HashMap<String, Object>();
            localCopyOfProperty.putAll(prop);
            final Map<String, Object> formatterProperties = null;
            final UUID inputStream = null;
            final String distributionID = null;
            writer.initDatabaseWriter(localCopyOfProperty, formatterProperties, inputStream, distributionID);
            final UUID sourceuuid = null;
            final HDEvent InsertEvent = new HDEvent(28, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "WA.SAMPLES_JUSTIN.TEST_SQLMX");
            final Object[] data = { "aa", "abcde ", "fghijk", "lmnopqrstuv", "xxxx ", "zzzz", "-20.123", "30.45", 400, -41, Integer.MAX_VALUE, -51, 8989898989L, "123", "-456", "12345678901234567890", "-98765432109876543210", "567890123456789012.34", "-765432109876543210.98", 100.7, 101.8, 202.16, "2001-5-1", "10:20:00", "20:30:40.50", 809975472000L, 1334539160224L, 946674855334L };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            int i = 0;
            for (final Object o : data) {
                System.out.println(i);
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            writer.closeDatabaseWriter();
        }
        catch (Exception e1) {
            throw e1;
        }
        finally {
            writer.closeDatabaseWriter();
        }
    }
    
    public static void main1(final String[] args) throws Exception {
        final Map<String, Object> prop = new HashMap<String, Object>();
        prop.put("Username", "scott");
        prop.put("Password", "tiger");
        prop.put("ConnectionURL", "jdbc:oracle:thin:@10.211.55.3:1521:orcl1");
        prop.put("Tables", "SCOTT.SIMPLETEST1,SCOTT.SIMPLETESTTARGET1;");
        prop.put("BatchPolicy", "Count:1 , Interval:1000");
        final DatabaseWriterNew writer = new DatabaseWriterNew();
        try {
            final Map<String, Object> localCopyOfProperty = new HashMap<String, Object>();
            localCopyOfProperty.putAll(prop);
            final Map<String, Object> formatterProperties = null;
            final UUID inputStream = null;
            final String distributionID = null;
            writer.initDatabaseWriter(localCopyOfProperty, formatterProperties, inputStream, distributionID);
            final UUID sourceuuid = null;
            HDEvent InsertEvent = new HDEvent(3, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            Object[] data = { "1", "row1simpleTest1", "abc" };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            int i = 0;
            for (final Object o : data) {
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            InsertEvent = new HDEvent(3, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            data = new Object[] { "2", "row1simpleTest1", "abc" };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            i = 0;
            for (final Object o : data) {
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            HDEvent UpdateEvent = new HDEvent(3, sourceuuid);
            (UpdateEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            Object[] dataUpdateBefore = { "1", "row1simpleTest1", "abc" };
            UpdateEvent.before = new Object[dataUpdateBefore.length];
            UpdateEvent.beforePresenceBitMap = new byte[(dataUpdateBefore.length + 7) / 8];
            int iub = 0;
            for (final Object o : dataUpdateBefore) {
                UpdateEvent.setBefore(iub++, o);
            }
            Object[] dataUpdate = { "1", "row1simpleTest1updated", "abcde" };
            UpdateEvent.data = new Object[dataUpdate.length];
            UpdateEvent.dataPresenceBitMap = new byte[(dataUpdate.length + 7) / 8];
            int iu = 0;
            for (final Object o2 : dataUpdate) {
                UpdateEvent.setData(iu++, o2);
            }
            UpdateEvent.metadata.put("OperationName", "UPDATE");
            writer.processEvent(1, (Event)UpdateEvent, null);
            UpdateEvent = new HDEvent(3, sourceuuid);
            (UpdateEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            dataUpdateBefore = new Object[] { "2", "row1simpleTest1", "abc" };
            UpdateEvent.before = new Object[dataUpdateBefore.length];
            UpdateEvent.beforePresenceBitMap = new byte[(dataUpdateBefore.length + 7) / 8];
            iub = 0;
            for (final Object o : dataUpdateBefore) {
                UpdateEvent.setBefore(iub++, o);
            }
            dataUpdate = new Object[] { "2", "row1simpleTest1updated", "abcde" };
            UpdateEvent.data = new Object[dataUpdate.length];
            UpdateEvent.dataPresenceBitMap = new byte[(dataUpdate.length + 7) / 8];
            iu = 0;
            for (final Object o2 : dataUpdate) {
                UpdateEvent.setData(iu++, o2);
            }
            UpdateEvent.metadata.put("OperationName", "UPDATE");
            writer.processEvent(1, (Event)UpdateEvent, null);
            HDEvent DeleteEvent = new HDEvent(3, sourceuuid);
            (DeleteEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            Object[] dataDelete = { "1", "row1simpleTest1updated", "abcde" };
            DeleteEvent.data = new Object[dataDelete.length];
            DeleteEvent.dataPresenceBitMap = new byte[(dataDelete.length + 7) / 8];
            int id = 0;
            for (final Object o : dataDelete) {
                DeleteEvent.setData(id++, o);
            }
            DeleteEvent.metadata.put("OperationName", "DELETE");
            writer.processEvent(1, (Event)DeleteEvent, null);
            DeleteEvent = new HDEvent(3, sourceuuid);
            (DeleteEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            dataDelete = new Object[] { "2", "row1simpleTest1updated", "abcde" };
            DeleteEvent.data = new Object[dataDelete.length];
            DeleteEvent.dataPresenceBitMap = new byte[(dataDelete.length + 7) / 8];
            id = 0;
            for (final Object o : dataDelete) {
                DeleteEvent.setData(id++, o);
            }
            DeleteEvent.metadata.put("OperationName", "DELETE");
            writer.processEvent(1, (Event)DeleteEvent, null);
            InsertEvent = new HDEvent(3, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            data = new Object[] { "1", "row1simpleTest1", "abc" };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            i = 0;
            for (final Object o : data) {
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            InsertEvent = new HDEvent(3, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            data = new Object[] { "2", "row1simpleTest1", "abc" };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            i = 0;
            for (final Object o : data) {
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            InsertEvent = new HDEvent(3, sourceuuid);
            (InsertEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            data = new Object[] { "3", "row1simpleTest1", "abc" };
            InsertEvent.data = new Object[data.length];
            InsertEvent.dataPresenceBitMap = new byte[(data.length + 7) / 8];
            i = 0;
            for (final Object o : data) {
                InsertEvent.setData(i++, o);
            }
            InsertEvent.metadata.put("OperationName", "INSERT");
            writer.processEvent(1, (Event)InsertEvent, null);
            DeleteEvent = new HDEvent(3, sourceuuid);
            (DeleteEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            dataDelete = new Object[] { "1", "row1simpleTest1", "abc" };
            DeleteEvent.data = new Object[dataDelete.length];
            DeleteEvent.dataPresenceBitMap = new byte[(dataDelete.length + 7) / 8];
            id = 0;
            for (final Object o : dataDelete) {
                DeleteEvent.setData(id++, o);
            }
            DeleteEvent.metadata.put("OperationName", "DELETE");
            writer.processEvent(1, (Event)DeleteEvent, null);
            DeleteEvent = new HDEvent(3, sourceuuid);
            (DeleteEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            dataDelete = new Object[] { "2", "row1simpleTest1", "abc" };
            DeleteEvent.data = new Object[dataDelete.length];
            DeleteEvent.dataPresenceBitMap = new byte[(dataDelete.length + 7) / 8];
            id = 0;
            for (final Object o : dataDelete) {
                DeleteEvent.setData(id++, o);
            }
            DeleteEvent.metadata.put("OperationName", "DELETE");
            writer.processEvent(1, (Event)DeleteEvent, null);
            DeleteEvent = new HDEvent(3, sourceuuid);
            (DeleteEvent.metadata = new HashMap()).put("TableName", "SCOTT.SIMPLETEST1");
            dataDelete = new Object[] { "3", "row1simpleTest1", "abc" };
            DeleteEvent.data = new Object[dataDelete.length];
            DeleteEvent.dataPresenceBitMap = new byte[(dataDelete.length + 7) / 8];
            id = 0;
            for (final Object o : dataDelete) {
                DeleteEvent.setData(id++, o);
            }
            DeleteEvent.metadata.put("OperationName", "DELETE");
            writer.processEvent(1, (Event)DeleteEvent, null);
            writer.closeDatabaseWriter();
        }
        catch (Exception e1) {
            throw e1;
        }
        finally {
            writer.closeDatabaseWriter();
        }
    }
    
    static {
        DatabaseWriterProc.logger = LoggerFactory.getLogger((Class)DatabaseWriterProc.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
