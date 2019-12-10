package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.type.*;

public class OracleColumn extends DatabaseColumn
{
    @Override
    public void setDataTypeName(final String string) {
        this.dataTypeName = string;
        final String dataTypeName = this.dataTypeName;
        switch (dataTypeName) {
            case "VARCHAR2": {
                this.typeCode = 1;
                break;
            }
            case "FLOAT":
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
            case "TIMESTAMP":
            case "TIMESTAMP(0)":
            case "TIMESTAMP(1)":
            case "TIMESTAMP(2)":
            case "TIMESTAMP(3)":
            case "TIMESTAMP(4)":
            case "TIMESTAMP(5)":
            case "TIMESTAMP(6)":
            case "TIMESTAMP(7)":
            case "TIMESTAMP(8)":
            case "TIMESTAMP(9)": {
                this.typeCode = 180;
                break;
            }
            case "TIMESTAMP(0) WITH TIME ZONE":
            case "TIMESTAMP(1) WITH TIME ZONE":
            case "TIMESTAMP(2) WITH TIME ZONE":
            case "TIMESTAMP(3) WITH TIME ZONE":
            case "TIMESTAMP(4) WITH TIME ZONE":
            case "TIMESTAMP(5) WITH TIME ZONE":
            case "TIMESTAMP(6) WITH TIME ZONE":
            case "TIMESTAMP(7) WITH TIME ZONE":
            case "TIMESTAMP(8) WITH TIME ZONE":
            case "TIMESTAMP(9) WITH TIME ZONE": {
                this.typeCode = 181;
                break;
            }
            case "INTERVAL YEAR(0) TO MONTH":
            case "INTERVAL YEAR(1) TO MONTH":
            case "INTERVAL YEAR(2) TO MONTH":
            case "INTERVAL YEAR(3) TO MONTH":
            case "INTERVAL YEAR(4) TO MONTH":
            case "INTERVAL YEAR(5) TO MONTH":
            case "INTERVAL YEAR(6) TO MONTH":
            case "INTERVAL YEAR(7) TO MONTH":
            case "INTERVAL YEAR(8) TO MONTH":
            case "INTERVAL YEAR(9) TO MONTH": {
                this.typeCode = 182;
                break;
            }
            case "INTERVAL DAY(0) TO SECOND(0)":
            case "INTERVAL DAY(1) TO SECOND(0)":
            case "INTERVAL DAY(2) TO SECOND(0)":
            case "INTERVAL DAY(3) TO SECOND(0)":
            case "INTERVAL DAY(4) TO SECOND(0)":
            case "INTERVAL DAY(5) TO SECOND(0)":
            case "INTERVAL DAY(6) TO SECOND(0)":
            case "INTERVAL DAY(7) TO SECOND(0)":
            case "INTERVAL DAY(8) TO SECOND(0)":
            case "INTERVAL DAY(9) TO SECOND(0)":
            case "INTERVAL DAY(0) TO SECOND(1)":
            case "INTERVAL DAY(1) TO SECOND(1)":
            case "INTERVAL DAY(2) TO SECOND(1)":
            case "INTERVAL DAY(3) TO SECOND(1)":
            case "INTERVAL DAY(4) TO SECOND(1)":
            case "INTERVAL DAY(5) TO SECOND(1)":
            case "INTERVAL DAY(6) TO SECOND(1)":
            case "INTERVAL DAY(7) TO SECOND(1)":
            case "INTERVAL DAY(8) TO SECOND(1)":
            case "INTERVAL DAY(9) TO SECOND(1)":
            case "INTERVAL DAY(0) TO SECOND(2)":
            case "INTERVAL DAY(1) TO SECOND(2)":
            case "INTERVAL DAY(2) TO SECOND(2)":
            case "INTERVAL DAY(3) TO SECOND(2)":
            case "INTERVAL DAY(4) TO SECOND(2)":
            case "INTERVAL DAY(5) TO SECOND(2)":
            case "INTERVAL DAY(6) TO SECOND(2)":
            case "INTERVAL DAY(7) TO SECOND(2)":
            case "INTERVAL DAY(8) TO SECOND(2)":
            case "INTERVAL DAY(9) TO SECOND(2)":
            case "INTERVAL DAY(0) TO SECOND(3)":
            case "INTERVAL DAY(1) TO SECOND(3)":
            case "INTERVAL DAY(2) TO SECOND(3)":
            case "INTERVAL DAY(3) TO SECOND(3)":
            case "INTERVAL DAY(4) TO SECOND(3)":
            case "INTERVAL DAY(5) TO SECOND(3)":
            case "INTERVAL DAY(6) TO SECOND(3)":
            case "INTERVAL DAY(7) TO SECOND(3)":
            case "INTERVAL DAY(8) TO SECOND(3)":
            case "INTERVAL DAY(9) TO SECOND(3)":
            case "INTERVAL DAY(0) TO SECOND(4)":
            case "INTERVAL DAY(1) TO SECOND(4)":
            case "INTERVAL DAY(2) TO SECOND(4)":
            case "INTERVAL DAY(3) TO SECOND(4)":
            case "INTERVAL DAY(4) TO SECOND(4)":
            case "INTERVAL DAY(5) TO SECOND(4)":
            case "INTERVAL DAY(6) TO SECOND(4)":
            case "INTERVAL DAY(7) TO SECOND(4)":
            case "INTERVAL DAY(8) TO SECOND(4)":
            case "INTERVAL DAY(9) TO SECOND(4)":
            case "INTERVAL DAY(0) TO SECOND(5)":
            case "INTERVAL DAY(1) TO SECOND(5)":
            case "INTERVAL DAY(2) TO SECOND(5)":
            case "INTERVAL DAY(3) TO SECOND(5)":
            case "INTERVAL DAY(4) TO SECOND(5)":
            case "INTERVAL DAY(5) TO SECOND(5)":
            case "INTERVAL DAY(6) TO SECOND(5)":
            case "INTERVAL DAY(7) TO SECOND(5)":
            case "INTERVAL DAY(8) TO SECOND(5)":
            case "INTERVAL DAY(9) TO SECOND(5)":
            case "INTERVAL DAY(0) TO SECOND(6)":
            case "INTERVAL DAY(1) TO SECOND(6)":
            case "INTERVAL DAY(2) TO SECOND(6)":
            case "INTERVAL DAY(3) TO SECOND(6)":
            case "INTERVAL DAY(4) TO SECOND(6)":
            case "INTERVAL DAY(5) TO SECOND(6)":
            case "INTERVAL DAY(6) TO SECOND(6)":
            case "INTERVAL DAY(7) TO SECOND(6)":
            case "INTERVAL DAY(8) TO SECOND(6)":
            case "INTERVAL DAY(9) TO SECOND(6)":
            case "INTERVAL DAY(0) TO SECOND(7)":
            case "INTERVAL DAY(1) TO SECOND(7)":
            case "INTERVAL DAY(2) TO SECOND(7)":
            case "INTERVAL DAY(3) TO SECOND(7)":
            case "INTERVAL DAY(4) TO SECOND(7)":
            case "INTERVAL DAY(5) TO SECOND(7)":
            case "INTERVAL DAY(6) TO SECOND(7)":
            case "INTERVAL DAY(7) TO SECOND(7)":
            case "INTERVAL DAY(8) TO SECOND(7)":
            case "INTERVAL DAY(9) TO SECOND(7)":
            case "INTERVAL DAY(0) TO SECOND(8)":
            case "INTERVAL DAY(1) TO SECOND(8)":
            case "INTERVAL DAY(2) TO SECOND(8)":
            case "INTERVAL DAY(3) TO SECOND(8)":
            case "INTERVAL DAY(4) TO SECOND(8)":
            case "INTERVAL DAY(5) TO SECOND(8)":
            case "INTERVAL DAY(6) TO SECOND(8)":
            case "INTERVAL DAY(7) TO SECOND(8)":
            case "INTERVAL DAY(8) TO SECOND(8)":
            case "INTERVAL DAY(9) TO SECOND(8)":
            case "INTERVAL DAY(0) TO SECOND(9)":
            case "INTERVAL DAY(1) TO SECOND(9)":
            case "INTERVAL DAY(2) TO SECOND(9)":
            case "INTERVAL DAY(3) TO SECOND(9)":
            case "INTERVAL DAY(4) TO SECOND(9)":
            case "INTERVAL DAY(5) TO SECOND(9)":
            case "INTERVAL DAY(6) TO SECOND(9)":
            case "INTERVAL DAY(7) TO SECOND(9)":
            case "INTERVAL DAY(8) TO SECOND(9)":
            case "INTERVAL DAY(9) TO SECOND(9)": {
                this.typeCode = 183;
                break;
            }
            case "UROWID": {
                this.typeCode = 208;
                break;
            }
            case "TIMESTAMP(0) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(1) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(2) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(3) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(4) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(5) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(6) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(7) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(8) WITH LOCAL TIME ZONE":
            case "TIMESTAMP(9) WITH LOCAL TIME ZONE": {
                this.typeCode = 231;
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
                this.dataTypeName = "VARCHAR2";
                break;
            }
            case 2: {
                this.dataTypeName = "NUMBER";
                break;
            }
            case 3: {
                this.dataTypeName = "NVARCHAR2";
                break;
            }
            case 8: {
                this.dataTypeName = "LONG";
                break;
            }
            case 12: {
                this.dataTypeName = "DATE";
                break;
            }
            case 23: {
                this.dataTypeName = "RAW";
                break;
            }
            case 24: {
                this.dataTypeName = "LONG RAW";
                break;
            }
            case 69: {
                this.dataTypeName = "ROWID";
                break;
            }
            case 96: {
                this.dataTypeName = "CHAR";
                break;
            }
            case 97: {
                this.dataTypeName = "NCHAR";
                break;
            }
            case 100: {
                this.dataTypeName = "BINARY_FLOAT";
                break;
            }
            case 101: {
                this.dataTypeName = "BINARY_DOUBLE";
                break;
            }
            case 111: {
                this.dataTypeName = "NCLOB";
                break;
            }
            case 112: {
                this.dataTypeName = "CLOB";
                break;
            }
            case 113: {
                this.dataTypeName = "BLOB";
                break;
            }
            case 114: {
                this.dataTypeName = "BFILE";
                break;
            }
            case 180: {
                this.dataTypeName = "TIMESTAMP";
                break;
            }
            case 181: {
                this.dataTypeName = "TIMESTAMP WITH TIME ZONE";
                break;
            }
            case 182: {
                this.dataTypeName = "INTERVAL YEAR TO MONTH";
                break;
            }
            case 183: {
                this.dataTypeName = "INTERVAL DAY TO SECOND";
                break;
            }
            case 208: {
                this.dataTypeName = "UROWID";
                break;
            }
            case 231: {
                this.dataTypeName = "TIMESTAMP WITH LOCAL TIME ZONE";
                break;
            }
            default: {
                this.dataTypeName = "";
                break;
            }
        }
    }
    
    @Override
    public void setInternalColumnType(final String dataTypeName) {
        if (dataTypeName.equalsIgnoreCase("ROWID") || dataTypeName.equalsIgnoreCase("UROWID")) {
            this.internalType = columntype.HD_STRING;
        }
        if (dataTypeName.equalsIgnoreCase("NUMBER")) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("FLOAT")) {
            this.internalType = columntype.HD_FLOAT;
        }
        else if (dataTypeName.equalsIgnoreCase("LONG")) {
            this.internalType = columntype.HD_LONG;
        }
        else if (dataTypeName.startsWith("INTERVAL DAY", 0)) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.startsWith("INTERVAL YEAR", 0)) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("NCHAR") || dataTypeName.equalsIgnoreCase("NVARCHAR2")) {
            this.internalType = columntype.HD_UTF16_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("CHAR") || dataTypeName.equalsIgnoreCase("VARCHAR2")) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("DATE") || dataTypeName.startsWith("TIMESTAMP", 0)) {
            this.internalType = columntype.HD_DATETIME;
        }
        else if (dataTypeName.equalsIgnoreCase("BINARY_FLOAT")) {
            this.internalType = columntype.HD_FLOAT;
        }
        else if (dataTypeName.equalsIgnoreCase("BINARY_DOUBLE")) {
            this.internalType = columntype.HD_DOUBLE;
        }
        else if (dataTypeName.equalsIgnoreCase("RAW") || dataTypeName.equalsIgnoreCase("LONG RAW")) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("BLOB")) {
            this.internalType = columntype.HD_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("CLOB") || dataTypeName.equalsIgnoreCase("NCLOB")) {
            this.internalType = columntype.HD_STRING;
        }
        else {
            this.internalType = columntype.HD_STRING;
        }
    }
    
    @Override
    public Column clone() {
        return new OracleColumn();
    }
}
