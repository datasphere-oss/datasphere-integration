package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.uuid.UUID;
import com.datasphere.BitPattern.BitPatternMap;
import com.datasphere.Table.Table;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.TypeHandler.TypeHandler;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.UnsupportedMapping;
import com.datasphere.exception.UpdateTypeMapException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class DeleteHandler extends OperationHandler {
	private Logger logger;

	public DeleteHandler(final DBInterface intf) {
		super(intf);
		this.logger = LoggerFactory.getLogger((Class) DeleteHandler.class);
	}

	@Override
	public void validate(final HDEvent event) throws DatabaseWriterException {
		super.validate(event);
	}

	@Override
	public void bind(final String targetTable, final HDEvent event, final PreparedStatement stmt)
			throws SQLException, DatabaseWriterException {
		final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTable, event);
		TypeHandler[] columns = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
		int itr = 0;
		int bindIndex = 0;
		int columnIndex = 0;
		final Object[] data = event.data;
		final Integer[] dataFiled = BitPatternMap.getPattern(event, event.dataPresenceBitMap);

		while (true) {
			try {
				bindIndex = 0;
				for (itr = 0; itr < dataFiled.length; ++itr) {
					columnIndex = dataFiled[itr];
					if (data[columnIndex] != null) {
						columns[columnIndex].bind(stmt, ++bindIndex, data[columnIndex]);
					}
				}
			} catch (UpdateTypeMapException exp) {
				try {
					for (int xitr = itr; xitr < dataFiled.length; ++xitr) {
						columnIndex = dataFiled[xitr];
						if (data[columnIndex] != null) {
							columns = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, columnIndex,
									data[columnIndex].getClass());
						}
					}
				} catch (UnsupportedMapping mappingExp) {
					final String srcTable = (String) event.metadata.get(Constant.TABLE_NAME);
					final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {"
							+ mappingExp.targetType + "} is not supported. Source Table {" + srcTable
							+ "} Target Table {" + targetTable + "}, Column index {" + columnIndex + "}";
					throw new DatabaseWriterException(errMsg);
				}
				continue;
			} catch (Exception uExp) {
				this.logger.error("Unhandled exception occured while biding data ", (Throwable) uExp);
				this.logger.error("Event caused this issue = " + event.toJSON());
				final Integer[] afterImageCols = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
				final Integer[] beforeIamgeCols = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
				this.logger.error("After Image column referred " + Arrays.toString((Object[]) afterImageCols));
				this.logger.error("Before Image column referred " + Arrays.toString((Object[]) beforeIamgeCols));
				try {
					final String debugQuery = this.generateDML(event, targetTable);
					this.logger.error("Debug SQL Statement {" + debugQuery + "}");
				} catch (Exception ex) {
				}
				throw uExp;
			}
			break;
		}
	}

	@Override
	public String generateDML(final HDEvent event, final String targetTable)
			throws SQLException, DatabaseWriterException {
		final StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(TableToTypeHandlerMap.formKey(targetTable, event));
        keyBuilder.append("-");
        keyBuilder.append(new String(event.dataPresenceBitMap));
        keyBuilder.append("-");
        keyBuilder.append(new String(event.beforePresenceBitMap));
        keyBuilder.append("-");
        keyBuilder.append(this.nullPattern(event, event.dataPresenceBitMap));
        final String key = keyBuilder.toString();
		boolean isMySQL = false;
		if (event.metadata.get("TargetType") != null) {
			if ("MySQL".equalsIgnoreCase(event.metadata.get("TargetType").toString())) {
				isMySQL = true;
			}
		}
		String query = this.statementCache.get(key);
		if (query == null) {
			String table = targetTable;
			if (!isMySQL) {
				if (table.indexOf(".") > -1) {
					String[] tabs = table.split("\\.");
					if (tabs.length == 2) {
						table = tabs[0] + ".\"" + tabs[1] + "\"";
					}
				}
			}
			final StringBuilder queryBuilder = new StringBuilder();
			final StringBuilder whereClause = new StringBuilder();
			final Table tableDef = (Table) TargetTableMap.getInstance(this.callbackIntf).getTableMeta(targetTable);
			final DatabaseColumn[] cols = tableDef.getColumns();
			queryBuilder.append("DELETE FROM ");
			queryBuilder.append(table);
			queryBuilder.append(" WHERE ");
			if (!isMySQL) {
				final Integer[] colIndex = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
				for (int itr = 0; itr < colIndex.length; ++itr) {
					if (itr != 0) {
						whereClause.append(" AND ");
					}
					if (!isMySQL) {
						whereClause.append("\"" + cols[colIndex[itr]].getName() + "\"");
					} else {
						whereClause.append("" + cols[colIndex[itr]].getName() + "");
					}
					if (event.data[colIndex[itr]] != null) {
						whereClause.append(" = ? ");
					} else {
						whereClause.append(" IS NULL ");
					}
				}
				queryBuilder.append((CharSequence) whereClause);
				query = queryBuilder.toString();
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Query prepared and cahced {" + query + "}");
				}
			} else {
				final Integer[] beforeBitPtrn = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
				for (int itr2 = 0; itr2 < event.data.length; ++itr2) {
					if (itr2 != 0) {
						whereClause.append(" and ");
					}
					
					whereClause.append(cols[beforeBitPtrn[itr2]].getName());
					if (event.data[beforeBitPtrn[itr2]] != null) {
						whereClause.append(" = ? ");
					} else {
						whereClause.append(" is null ");
					}
				}
				queryBuilder.append((CharSequence) whereClause);
				query = queryBuilder.toString();
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Query prepared and cahced {" + query + "}");
				}
			}
			this.statementCache.put(key, query);
			String keys = this.statementCache.get(targetTable);
			if (keys == null) {
				keys = key;
			} else {
				keys = keys + ";" + key;
			}
			this.statementCache.put(targetTable, keys);
		}
		return query;
	}

	class SecurityAccess {
		public void disopen() {

		}
	}

}
