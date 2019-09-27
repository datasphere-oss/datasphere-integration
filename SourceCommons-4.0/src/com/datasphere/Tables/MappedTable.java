package com.datasphere.Tables;

import java.sql.*;

public class MappedTable extends Table
{
    public MappedTable(final ResultSet tableInfo, final ResultSet metaResultSet, final ResultSet keyResultSet) throws SQLException {
        super(tableInfo, metaResultSet, keyResultSet);
    }
}
