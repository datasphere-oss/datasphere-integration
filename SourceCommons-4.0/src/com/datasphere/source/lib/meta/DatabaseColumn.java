package com.datasphere.source.lib.meta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.Fetchers.Fetcher;
import com.datasphere.source.lib.type.columntype;

public abstract class DatabaseColumn extends Column
{
    public String dataTypeName;
    int dataLength;
    int precision;
    int scale;
    public String nullable;
    boolean isKey;
    public int typeCode;
    boolean isExcluded;
    boolean isValReplaced;
    String replacedValue;
    private boolean isNullable;
    private static Logger logger;
    public columntype internalType;
    private static Map<String, Integer> typeMap;
    private final String COLUMN_NAME = "COLUMN_NAME";
    private final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private final String ORDINAL_POSITION = "ORDINAL_POSITION";
    private final String DATA_TYPE = "DATA_TYPE";
    private final String COLUMN_SIZE = "COLUMN_SIZE";
    private final String IS_NULLABLE = "IS_NULLABLE";
    private final String TYPE_NAME = "TYPE_NAME";
    private String typeName;
    protected Fetcher fetcher;
    
    public DatabaseColumn() {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.replacedValue = null;
    }
    
    public String getDataTypeName() {
        return this.dataTypeName;
    }
    
    public int getDataLength() {
        return this.dataLength;
    }
    
    public int getPrecision() {
        return this.precision;
    }
    
    public int getScale() {
        return this.scale;
    }
    
    public boolean isNullable() {
        return true;
    }
    
    public boolean isKey() {
        return this.isKey;
    }
    
    public boolean isValueReplaced() {
        return this.isValReplaced;
    }
    
    public String getReplacedValue() {
        return this.replacedValue;
    }
    
    public boolean isExcluded() {
        return this.isExcluded;
    }
    
    @Override
    public void setIndex(final int index) {
        this.index = index;
    }
    
    @Override
    public void setName(final String name) {
        this.name = name;
    }
    
    public void setDataTypeName(final String string) {
        this.dataTypeName = string;
        final String dataTypeName = this.dataTypeName;
        switch (dataTypeName) {
            case "VARCHAR2": {
                this.typeCode = 1;
                break;
            }
            case "NUMBER": {
                this.typeCode = 2;
                break;
            }
            case "NVARCHAR2": {
                this.typeCode = 3;
                break;
            }
            case "LONG": {
                this.typeCode = 8;
                break;
            }
            case "DATE": {
                this.typeCode = 12;
                break;
            }
            case "RAW": {
                this.typeCode = 23;
                break;
            }
            case "LONG RAW": {
                this.typeCode = 24;
                break;
            }
            case "ROWID": {
                this.typeCode = 69;
                break;
            }
            case "CHAR": {
                this.typeCode = 96;
                break;
            }
            case "NCHAR": {
                this.typeCode = 97;
                break;
            }
            case "BINARY_FLOAT": {
                this.typeCode = 100;
                break;
            }
            case "BINARY_DOUBLE": {
                this.typeCode = 101;
                break;
            }
            case "NCLOB": {
                this.typeCode = 111;
                break;
            }
            case "CLOB": {
                this.typeCode = 112;
                break;
            }
            case "BLOB": {
                this.typeCode = 113;
                break;
            }
            case "BFILE": {
                this.typeCode = 114;
                break;
            }
            case "TIMESTAMP(6)": {
                this.typeCode = 180;
                break;
            }
            case "TIMESTAMP(6) WITH TIME ZONE": {
                this.typeCode = 181;
                break;
            }
            case "INTERVAL YEAR TO MONTH": {
                this.typeCode = 182;
                break;
            }
            case "INTERVAL DAY TO SECOND": {
                this.typeCode = 183;
                break;
            }
            case "UROWID": {
                this.typeCode = 208;
                break;
            }
            case "TIMESTAMP(6) WITH LOCAL TIME ZONE": {
                this.typeCode = 231;
                break;
            }
        }
    }
    
    public void setDataLength(final int dataLength) {
        this.dataLength = dataLength;
    }
    
    public void setPrecision(final int precision) {
        this.precision = precision;
    }
    
    public void setScale(final int scale) {
        this.scale = scale;
    }
    
    public void setNullable(final String string) {
        this.nullable = string;
    }
    
    public void setKey(final boolean isKey) {
        this.isKey = isKey;
    }
    
    public void setIsValReplaced(final boolean isRepalced) {
        this.isValReplaced = isRepalced;
    }
    
    public void setReplacedValue(final String replValue) {
        this.replacedValue = replValue;
    }
    
    public void setExcluded(final boolean isExcluded) {
        this.isExcluded = isExcluded;
    }
    
    public int getTypeCode() {
        return this.typeCode;
    }
    
    public void setTypeCode(final int typeCode) {
        this.typeCode = typeCode;
    }
    
    public columntype getInternalColumnType() {
        return this.internalType;
    }
    
    public abstract void setInternalColumnType(final String p0);
    
    public static DatabaseColumn initializeDataBaseColumnFromProductName(final String dbProductName) {
        if (dbProductName == null) {
            return null;
        }
        final String nameToCheck = dbProductName.toLowerCase();
        if (nameToCheck.indexOf("oracle") != -1) {
            return new OracleColumn();
        }
        if (nameToCheck.indexOf("mysql") != -1) {
            return new MySQLColumn();
        }
        if (nameToCheck.indexOf("sql server") != -1) {
            return new MSSqlColumn();
        }
        if (nameToCheck.indexOf("sql/mx") != -1) {
            return null;
        }
        DatabaseColumn.logger.warn("Database Product Name is not known: " + dbProductName);
        return null;
    }
    
    public DatabaseColumn(final String name, final Class<?> type, final int index, final boolean isKey) {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.setName(name);
        this.setDataTypeName(type.getName());
        this.setKey(isKey);
        this.setIndex(index);
    }
    
    public DatabaseColumn(final String name, final String typeName, final int index) {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.setName(name);
        this.setDataTypeName(typeName);
        this.setIndex(index);
        this.setKey(false);
    }
    
    public DatabaseColumn(final ResultSet columnInfo) throws SQLException {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.setName(columnInfo.getString("COLUMN_NAME"));
        this.setDataLength(columnInfo.getInt("COLUMN_SIZE"));
        this.setIndex(columnInfo.getInt("ORDINAL_POSITION") - 1);
        this.setPrecision(columnInfo.getInt("DECIMAL_DIGITS"));
        this.setTypeCode(columnInfo.getInt("DATA_TYPE"));
        this.isNullable = "YES".equalsIgnoreCase(columnInfo.getString("IS_NULLABLE"));
        this.setDataTypeName(this.typeName = columnInfo.getString("TYPE_NAME"));
    }
    
    public DatabaseColumn(final DatabaseColumn org) {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.setName(org.getName());
        this.setDataLength(org.getDataLength());
        this.setIndex(org.getIndex());
        this.setPrecision(org.getPrecision());
        this.setTypeCode(org.getTypeCode());
        this.isNullable = org.isNullable();
        this.fetcher = org.fetcher;
        this.typeName = org.typeName;
        this.dataLength = org.dataLength;
        this.dataTypeName = org.dataTypeName;
        this.isKey = org.isKey;
        this.isExcluded = org.isExcluded;
    }
    
    public DatabaseColumn(final JSONObject columnInfo) throws JSONException {
        this.dataTypeName = null;
        this.dataLength = 0;
        this.precision = 0;
        this.scale = 0;
        this.nullable = null;
        this.isKey = false;
        this.typeCode = 0;
        this.isExcluded = false;
        this.isValReplaced = false;
        this.setName(columnInfo.getString("ColumnName"));
        this.setDataLength(columnInfo.getInt("ColumnLength"));
        this.setIndex(columnInfo.getInt("ColumnIndex"));
        this.setPrecision(columnInfo.getInt("ColumnPrecision"));
        this.typeCode = stringToSQLType(columnInfo.getString("ColumnType"));
        this.isNullable = columnInfo.getBoolean("ColumnIsNullable");
        this.setKey(columnInfo.getBoolean("ColumnIsKey"));
        this.typeName = null;
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
    
    public String typeName() {
        return this.typeName;
    }
    
    public static int stringToSQLType(final String sql) {
        final Integer sqlType = DatabaseColumn.typeMap.get(sql);
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
    
    static {
        DatabaseColumn.logger = LoggerFactory.getLogger((Class)DatabaseColumn.class);
        DatabaseColumn.typeMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER) {
            private static final long serialVersionUID = 1L;
            
            {
                this.put("BIT", -7);
                this.put("TINYINT", -6);
                this.put("SMALLINT", 5);
                this.put("INTEGER", 4);
                this.put("BIGINT", -5);
                this.put("FLOAT", 6);
                this.put("REAL", 7);
                this.put("DOUBLE", 8);
                this.put("NUMERIC", 2);
                this.put("DECIMAL", 3);
                this.put("CHAR", 1);
                this.put("VARCHAR", 12);
                this.put("LONGVARCHAR", -1);
                this.put("DATE", 91);
                this.put("TIME", 92);
                this.put("TIMESTAMP", 93);
                this.put("BINARY", -2);
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
}
