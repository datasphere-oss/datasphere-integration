package com.datasphere.DBCommons;

import java.sql.*;

public interface TableMetaProvider
{
    DatabaseMetaData getMetaData() throws SQLException;
}
