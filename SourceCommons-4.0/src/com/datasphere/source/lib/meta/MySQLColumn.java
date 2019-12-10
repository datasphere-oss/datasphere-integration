package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.type.*;

public class MySQLColumn extends DatabaseColumn
{
    @Override
    public void setDataTypeName(final String string) {
        this.dataTypeName = string;
        final String dataTypeName = this.dataTypeName;
        switch (dataTypeName) {
            case "BIT": {
                this.typeCode = 1;
                break;
            }
            case "TINYINT": {
                this.typeCode = 2;
                break;
            }
            case "SMALLINT": {
                this.typeCode = 3;
                break;
            }
            case "MEDIUMINT": {
                this.typeCode = 4;
                break;
            }
            case "INT":
            case "INTEGER": {
                this.typeCode = 5;
                break;
            }
            case "BIGINT": {
                this.typeCode = 6;
                break;
            }
            case "TINYINT UNSIGNED": {
                this.typeCode = 7;
                break;
            }
            case "SMALLINT UNSIGNED": {
                this.typeCode = 8;
                break;
            }
            case "MEDIUMINT UNSIGNED": {
                this.typeCode = 9;
                break;
            }
            case "INT UNSIGNED":
            case "INTEGER UNSIGNED": {
                this.typeCode = 10;
                break;
            }
            case "BIGINT UNSIGNED": {
                this.typeCode = 11;
                break;
            }
            case "REAL":
            case "FLOAT": {
                this.typeCode = 12;
                break;
            }
            case "DOUBLE": {
                this.typeCode = 13;
                break;
            }
            case "REAL UNSIGNED":
            case "FLOAT UNSIGNED": {
                this.typeCode = 14;
                break;
            }
            case "DOUBLE UNSIGNED": {
                this.typeCode = 15;
                break;
            }
            case "DECIMAL":
            case "NUMERIC": {
                this.typeCode = 16;
                break;
            }
            case "DECIMAL UNSIGNED":
            case "NUMERIC UNSIGNED": {
                this.typeCode = 20;
                break;
            }
            case "DATE": {
                this.typeCode = 24;
                break;
            }
            case "TIME": {
                this.typeCode = 25;
                break;
            }
            case "TIMESTAMP": {
                this.typeCode = 26;
                break;
            }
            case "DATETIME": {
                this.typeCode = 27;
                break;
            }
            case "YEAR": {
                this.typeCode = 28;
                break;
            }
            case "CHAR": {
                this.typeCode = 29;
                break;
            }
            case "VARCHAR": {
                this.typeCode = 30;
                break;
            }
            case "BINARY": {
                this.typeCode = 31;
                break;
            }
            case "VARBINARY": {
                this.typeCode = 32;
                break;
            }
            case "TINYBLOB": {
                this.typeCode = 33;
                break;
            }
            case "BLOB": {
                this.typeCode = 34;
                break;
            }
            case "MEDIUMBLOB": {
                this.typeCode = 35;
                break;
            }
            case "LONGBLOB": {
                this.typeCode = 36;
                break;
            }
            case "TINYTEXT": {
                this.typeCode = 37;
                break;
            }
            case "TEXT": {
                this.typeCode = 38;
                break;
            }
            case "MEDIUMTEXT": {
                this.typeCode = 39;
                break;
            }
            case "LONGTEXT": {
                this.typeCode = 40;
                break;
            }
            case "ENUM": {
                this.typeCode = 41;
                break;
            }
            case "SET": {
                this.typeCode = 42;
                break;
            }
            case "JSON": {
                this.typeCode = 43;
                this.typeCode = 0;
                break;
            }
            default: {
                this.typeCode = 0;
                break;
            }
        }
    }
    
    public void setDataType(final int type) {
        switch (this.typeCode = type) {
            case 1: {
                this.dataTypeName = "BIT";
                break;
            }
            case 2: {
                this.dataTypeName = "TINYINT";
                break;
            }
            case 3: {
                this.dataTypeName = "SMALLINT";
                break;
            }
            case 4: {
                this.dataTypeName = "MEDIUMINT";
                break;
            }
            case 5: {
                this.dataTypeName = "INT";
                break;
            }
            case 6: {
                this.dataTypeName = "BIGINT";
                break;
            }
            case 7: {
                this.dataTypeName = "TINYINT UNSIGNED";
                break;
            }
            case 8: {
                this.dataTypeName = "SMALLINT UNSIGNED";
                break;
            }
            case 9: {
                this.dataTypeName = "MEDIUMINT UNSIGNED";
                break;
            }
            case 10: {
                this.dataTypeName = "INT UNSIGNED";
                break;
            }
            case 11: {
                this.dataTypeName = "BIGINT UNSIGNED";
                break;
            }
            case 12: {
                this.dataTypeName = "REAL";
                break;
            }
            case 13: {
                this.dataTypeName = "DOUBLE";
                break;
            }
            case 14: {
                this.dataTypeName = "REAL UNSIGNED";
                break;
            }
            case 15: {
                this.dataTypeName = "DOUBLE UNSIGNED";
                break;
            }
            case 16: {
                this.dataTypeName = "DECIMAL";
                break;
            }
            case 20: {
                this.dataTypeName = "DECIMAL UNSIGNED";
                break;
            }
            case 24: {
                this.dataTypeName = "DATE";
                break;
            }
            case 25: {
                this.dataTypeName = "TIME";
                break;
            }
            case 26: {
                this.dataTypeName = "TIMESTAMP";
                break;
            }
            case 27: {
                this.dataTypeName = "DATETIME";
                break;
            }
            case 28: {
                this.dataTypeName = "YEAR";
                break;
            }
            case 29: {
                this.dataTypeName = "CHAR";
                break;
            }
            case 30: {
                this.dataTypeName = "VARCHAR";
                break;
            }
            case 31: {
                this.dataTypeName = "BINARY";
                break;
            }
            case 32: {
                this.dataTypeName = "VARBINARY";
                break;
            }
            case 33: {
                this.dataTypeName = "TINYBLOB";
                break;
            }
            case 34: {
                this.dataTypeName = "BLOB";
                break;
            }
            case 35: {
                this.dataTypeName = "MEDIUMBLOB";
                break;
            }
            case 36: {
                this.dataTypeName = "LONGBLOB";
                break;
            }
            case 37: {
                this.dataTypeName = "TINYTEXT";
                break;
            }
            case 38: {
                this.dataTypeName = "TEXT";
                break;
            }
            case 39: {
                this.dataTypeName = "MEDIUMTEXT";
                break;
            }
            case 40: {
                this.dataTypeName = "LONGTEXT";
                break;
            }
            case 41: {
                this.dataTypeName = "ENUM";
                break;
            }
            case 42: {
                this.dataTypeName = "SET";
                break;
            }
            case 43: {
                this.dataTypeName = "JSON";
                break;
            }
            default: {
                this.dataTypeName = "UNKNOWN";
                break;
            }
        }
    }
    
    @Override
    public void setInternalColumnType(final String dataTypeName) {
        switch (dataTypeName) {
            case "BIT": {
                this.internalType = columntype.HD_LONG;
                break;
            }
            case "TINYINT": {
                this.internalType = columntype.HD_SIGNED_BYTE;
                break;
            }
            case "SMALLINT": {
                this.internalType = columntype.HD_SIGNED_SHORT;
                break;
            }
            case "MEDIUMINT": {
                this.internalType = columntype.HD_SIGNED_INTEGER;
                break;
            }
            case "INT":
            case "INTEGER": {
                this.internalType = columntype.HD_SIGNED_INTEGER;
                break;
            }
            case "BIGINT": {
                this.internalType = columntype.HD_SIGNED_LONG;
                break;
            }
            case "TINYINT UNSIGNED": {
                this.internalType = columntype.HD_SHORT;
                break;
            }
            case "SMALLINT UNSIGNED": {
                this.internalType = columntype.HD_INTEGER;
                break;
            }
            case "MEDIUMINT UNSIGNED": {
                this.internalType = columntype.HD_INTEGER;
                break;
            }
            case "INT UNSIGNED":
            case "INTEGER UNSIGNED": {
                this.internalType = columntype.HD_LONG;
                break;
            }
            case "BIGINT UNSIGNED": {
                this.internalType = columntype.HD_LONG;
                break;
            }
            case "REAL":
            case "FLOAT": {
                this.internalType = columntype.HD_FLOAT;
                break;
            }
            case "DOUBLE": {
                this.internalType = columntype.HD_DOUBLE;
                break;
            }
            case "REAL UNSIGNED":
            case "FLOAT UNSIGNED": {
                this.internalType = columntype.HD_FLOAT;
                break;
            }
            case "DOUBLE UNSIGNED": {
                this.internalType = columntype.HD_DOUBLE;
                break;
            }
            case "DECIMAL":
            case "NUMERIC": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            case "DECIMAL UNSIGNED":
            case "NUMERIC UNSIGNED": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            case "DATE": {
                this.internalType = columntype.HD_DATE;
                break;
            }
            case "TIME": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            case "TIMESTAMP": {
                this.internalType = columntype.HD_DATETIME;
                break;
            }
            case "DATETIME": {
                this.internalType = columntype.HD_DATETIME;
                break;
            }
            case "YEAR": {
                this.internalType = columntype.HD_SHORT;
                break;
            }
            case "CHAR": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            case "VARCHAR": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            case "BINARY": {
                this.internalType = columntype.HD_BINARY;
                break;
            }
            case "VARBINARY": {
                this.internalType = columntype.HD_BINARY;
                break;
            }
            case "TINYBLOB": {
                this.internalType = columntype.HD_BLOB;
                break;
            }
            case "BLOB": {
                this.internalType = columntype.HD_BLOB;
                break;
            }
            case "MEDIUMBLOB": {
                this.internalType = columntype.HD_BLOB;
                break;
            }
            case "LONGBLOB": {
                this.internalType = columntype.HD_BLOB;
                break;
            }
            case "TINYTEXT": {
                this.internalType = columntype.HD_CLOB;
                break;
            }
            case "TEXT": {
                this.internalType = columntype.HD_CLOB;
                break;
            }
            case "MEDIUMTEXT": {
                this.internalType = columntype.HD_CLOB;
                break;
            }
            case "LONGTEXT": {
                this.internalType = columntype.HD_CLOB;
                break;
            }
            case "ENUM": {
                this.internalType = columntype.HD_INTEGER;
                break;
            }
            case "SET": {
                this.internalType = columntype.HD_LONG;
                break;
            }
            case "JSON": {
                this.internalType = columntype.HD_STRING;
                break;
            }
            default: {
                this.internalType = columntype.HD_UNSUPPORTED;
                break;
            }
        }
    }
    
    @Override
    public Column clone() {
        return new MySQLColumn();
    }
}
