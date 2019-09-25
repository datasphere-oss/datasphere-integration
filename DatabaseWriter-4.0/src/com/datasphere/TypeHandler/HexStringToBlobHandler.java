package com.datasphere.TypeHandler;

import com.datasphere.runtime.utils.*;
import java.sql.*;

class HexStringToBlobHandler extends TypeHandler
{
    public HexStringToBlobHandler() {
        super(OracleTypeHandler.BLOB, String.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setBytes(bindIndex, StringUtils.hexToRaw((String)value));
    }
}
