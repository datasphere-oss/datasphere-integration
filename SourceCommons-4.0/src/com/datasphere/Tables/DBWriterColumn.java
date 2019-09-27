package com.datasphere.Tables;

import com.datasphere.Fetchers.*;
import com.datasphere.source.lib.meta.*;

import java.sql.*;
import org.json.*;
import org.apache.kudu.*;
import java.util.*;

public class DBWriterColumn extends DatabaseColumn
{
    private static Map<String, Integer> typeMap;
    private final String COLUMN_NAME = "COLUMN_NAME";
    private final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private final String ORDINAL_POSITION = "ORDINAL_POSITION";
    private final String DATA_TYPE = "DATA_TYPE";
    private final String COLUMN_SIZE = "COLUMN_SIZE";
    private final String IS_NULLABLE = "IS_NULLABLE";
    private final String TYPE_NAME = "TYPE_NAME";
    private boolean isNullable;
    private String typeName;
    protected Fetcher fetcher;
    private int targetIndex;
    
    public DBWriterColumn(final String name, final Class<?> type, final int index, final boolean isKey) {
        this.setName(name);
        this.setDataTypeName(type.getName());
        this.setKey(isKey);
        this.setIndex(index);
    }
    
    public DBWriterColumn(final ResultSet columnInfo) throws SQLException {
        this.setName(columnInfo.getString("COLUMN_NAME"));
        this.setDataLength(columnInfo.getInt("COLUMN_SIZE"));
        this.setIndex(columnInfo.getInt("ORDINAL_POSITION") - 1);
        this.setPrecision(columnInfo.getInt("DECIMAL_DIGITS"));
        this.setTypeCode(columnInfo.getInt("DATA_TYPE"));
        this.isNullable = "YES".equalsIgnoreCase(columnInfo.getString("IS_NULLABLE"));
        this.typeName = columnInfo.getString("TYPE_NAME");
    }
    
    public DBWriterColumn(final DBWriterColumn org) {
        this.setName(org.getName());
        this.setDataLength(org.getDataLength());
        this.setIndex(org.getIndex());
        this.setPrecision(org.getPrecision());
        this.setTypeCode(org.getTypeCode());
        this.isNullable = org.isNullable();
        this.fetcher = org.fetcher;
        this.typeName = org.typeName;
        this.setKey(org.isKey());
        this.setTargetIndex(org.getTargetIndex());
    }
    
    public DBWriterColumn(final JSONObject columnInfo) throws JSONException {
        this.setName(columnInfo.getString("ColumnName"));
        this.setDataLength(columnInfo.getInt("ColumnLength"));
        this.setIndex(columnInfo.getInt("ColumnIndex"));
        this.setPrecision(columnInfo.getInt("ColumnPrecision"));
        this.typeCode = stringToSQLType(columnInfo.getString("ColumnType"));
        this.isNullable = columnInfo.getBoolean("ColumnIsNullable");
        this.setKey(columnInfo.getBoolean("ColumnIsKey"));
        this.typeName = null;
    }
    
    public DBWriterColumn(final ColumnSchema columnSchema, final int i) {
        this.setName(columnSchema.getName());
        this.setIndex(i);
        this.setKey(columnSchema.isKey());
        this.targetIndex = i;
        this.isNullable = columnSchema.isNullable();
        this.typeName = columnSchema.getType().getDataType().toString().toUpperCase();
        this.typeCode = stringToSQLType(this.typeName);
    }
    
    @Override
    public String toString() {
        final StringBuilder colDetails = new StringBuilder();
        colDetails.append("{COLUMN_NAME:" + this.getName() + "}");
        colDetails.append("{DATA_TYPE:" + this.typeCode + "}");
        colDetails.append("{COLUMN_SIZE:" + this.getSize() + "}");
        colDetails.append("{ORDINAL_POSITION:" + this.getIndex() + "}");
        colDetails.append("{DECIMAL_DIGITS:" + this.getPrecision() + "}");
        colDetails.append("{IS_NULLABLE:" + this.isNullable + "}");
        return colDetails.toString();
    }
    
    @Override
    public boolean isNullable() {
        return this.isNullable;
    }
    
    public String typeName() {
        return this.typeName;
    }
    
    @Override
    public void setInternalColumnType(final String dataTypeName) {
    }
    
    public static int stringToSQLType(final String sql) {
        final Integer sqlType = DBWriterColumn.typeMap.get(sql);
        if (sqlType != null) {
            return sqlType;
        }
        return -1;
    }
    
    public void setFetcher(final Fetcher fetcher) {
        this.fetcher = fetcher;
    }
    
    public Fetcher getFetcher() {
        return this.fetcher;
    }
    
    public int getTargetIndex() {
        return this.targetIndex;
    }
    
    public void setTargetIndex(final int index) {
        this.targetIndex = index;
    }
    
    static {
        DBWriterColumn.typeMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER) {
            private static final long serialVersionUID = 1L;
            
            {
                this.put("INT8", -6);
                this.put("INT16", 5);
                this.put("INT32", 4);
                this.put("INT64", -5);
                this.put("BINARY", -2);
                this.put("STRING", 12);
                this.put("BOOL", 16);
                this.put("FLOAT", 6);
                this.put("DOUBLE", 8);
                this.put("UNIXTIME_MICROS", 93);
                this.put("BIT", -7);
                this.put("TINYINT", -6);
                this.put("SMALLINT", 5);
                this.put("INTEGER", 4);
                this.put("BIGINT", -5);
                this.put("FLOAT", 6);
                this.put("REAL", 7);
                this.put("NUMERIC", 2);
                this.put("DECIMAL", 3);
                this.put("CHAR", 1);
                this.put("VARCHAR", 12);
                this.put("LONGVARCHAR", -1);
                this.put("DATE", 91);
                this.put("TIME", 92);
                this.put("TIMESTAMP", 93);
                this.put("VARBINARY", -3);
                this.put("LONGVARBINARY", -4);
                this.put("NULL", 0);
                this.put("OTHER", 1111);
                this.put("JAVA_OBJECT", 2000);
                this.put("DISTINCT", 2001);
                this.put("STRUCT", 2002);
                this.put("ARRAY", 2003);
                this.put("BLOB", 2004);
                this.put("CLOB", 2005);
                this.put("REF", 2006);
                this.put("DATALINK", 70);
                this.put("BOOLEAN", 16);
                this.put("ROWID", -8);
                this.put("NCHAR", -15);
                this.put("NVARCHAR", -9);
                this.put("LONGNVARCHAR", -16);
                this.put("NCLOB", 2011);
                this.put("SQLXML", 2009);
            }
        };
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
