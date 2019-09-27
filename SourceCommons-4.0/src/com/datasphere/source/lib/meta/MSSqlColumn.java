package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.type.*;

public class MSSqlColumn extends DatabaseColumn
{
    @Override
    public void setDataTypeName(final String string) {
        this.dataTypeName = string;
        this.typeCode = 0;
    }
    
    @Override
    public void setInternalColumnType(final String dataTypeName) {
        if (dataTypeName.equalsIgnoreCase("bigint")) {
            this.internalType = columntype.WA_LONG;
        }
        else if (dataTypeName.equalsIgnoreCase("binary") || dataTypeName.equalsIgnoreCase("image") || dataTypeName.equalsIgnoreCase("timestamp") || dataTypeName.equalsIgnoreCase("varbinary") || dataTypeName.equalsIgnoreCase("varbinary(max)")) {
            this.internalType = columntype.WA_BINARY;
        }
        else if (dataTypeName.equalsIgnoreCase("bit") || dataTypeName.equalsIgnoreCase("decimal") || dataTypeName.equalsIgnoreCase("money") || dataTypeName.equalsIgnoreCase("numeric") || dataTypeName.equalsIgnoreCase("smallmoney") || dataTypeName.equalsIgnoreCase("char") || dataTypeName.equalsIgnoreCase("nchar") || dataTypeName.equalsIgnoreCase("ntext") || dataTypeName.equalsIgnoreCase("nvarchar") || dataTypeName.equalsIgnoreCase("nvarchar(max)") || dataTypeName.equalsIgnoreCase("text") || dataTypeName.equalsIgnoreCase("varchar") || dataTypeName.equalsIgnoreCase("varchar(max)") || dataTypeName.equalsIgnoreCase("uniqueidentifier") || dataTypeName.equalsIgnoreCase("xml") || dataTypeName.equalsIgnoreCase("udt") || dataTypeName.equalsIgnoreCase("datetimeoffset") || dataTypeName.equalsIgnoreCase("time")) {
            this.internalType = columntype.WA_STRING;
        }
        else if (dataTypeName.equalsIgnoreCase("date")) {
            this.internalType = columntype.WA_DATE;
        }
        else if (dataTypeName.equalsIgnoreCase("datetime") || dataTypeName.equalsIgnoreCase("smalldatetime") || dataTypeName.equalsIgnoreCase("datetime2")) {
            this.internalType = columntype.WA_DATETIME;
        }
        else if (dataTypeName.equalsIgnoreCase("float")) {
            this.internalType = columntype.WA_DOUBLE;
        }
        else if (dataTypeName.equalsIgnoreCase("int")) {
            this.internalType = columntype.WA_INTEGER;
        }
        else if (dataTypeName.equalsIgnoreCase("real")) {
            this.internalType = columntype.WA_FLOAT;
        }
        else if (dataTypeName.equalsIgnoreCase("smallint") || dataTypeName.equalsIgnoreCase("tinyint")) {
            this.internalType = columntype.WA_SHORT;
        }
        else {
            this.internalType = columntype.WA_STRING;
        }
    }
    
    @Override
    public Column clone() {
        return new MSSqlColumn();
    }
    
    public boolean isColumnSupportsBeforeImageValue() {
        return this.dataTypeName.equalsIgnoreCase("binary") || this.dataTypeName.equalsIgnoreCase("image") || this.dataTypeName.equalsIgnoreCase("varbinary") || this.dataTypeName.equalsIgnoreCase("varbinary(max)") || this.dataTypeName.equalsIgnoreCase("ntext") || this.dataTypeName.equalsIgnoreCase("text");
    }
}
