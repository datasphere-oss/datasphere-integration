package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import org.json.*;
import java.sql.*;

public class CTASHandler extends CreateTableHandler
{
    public CTASHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        String tableName = null;
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final TCreateTableSqlStatement createStmt = (TCreateTableSqlStatement)this.sqlParser.sqlstatements.get(0);
            final TTableList tables = createStmt.tables;
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable t = tables.getTable(itr);
                final TObjectName oName = t.getTableName();
                final MappedName mappedTableName = this.getMappedName(catalog, schema, oName);
                mappedTableName.update(oName);
                tableName = mappedTableName.toString();
            }
            final StringBuilder createTableCmt = new StringBuilder();
            final String columnDetails = (String)event.metadata.get(Constant.TABLE_META_DATA);
            try {
                final JSONObject tblMeta = new JSONObject(columnDetails);
                final JSONArray columnDetais = tblMeta.getJSONArray("ColumnMetadata");
                createTableCmt.append("CREATE TABLE ");
                createTableCmt.append(tableName);
                createTableCmt.append(" ( ");
                for (int itr2 = 0; itr2 < columnDetais.length(); ++itr2) {
                    final JSONObject column = columnDetais.getJSONObject(itr2);
                    final String colName = column.getString("ColumnName");
                    final String colType = column.getString("ColumnType");
                    final int colLen = column.getInt("ColumnLength");
                    if (itr2 != 0) {
                        createTableCmt.append(", ");
                    }
                    createTableCmt.append(colName);
                    createTableCmt.append(" ");
                    createTableCmt.append(colType);
                    if (colLen != 0) {
                        createTableCmt.append("(");
                        createTableCmt.append(Integer.toString(colLen));
                        createTableCmt.append(")");
                    }
                }
                createTableCmt.append(" )");
            }
            catch (JSONException e) {
                throw new DatabaseWriterException("JSON Parse issue {" + e.getMessage() + "}");
            }
            ddlCmd = createTableCmt.toString();
            return ddlCmd;
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
