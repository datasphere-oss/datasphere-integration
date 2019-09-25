package com.datasphere.parser;

import java.util.ArrayList;
import java.util.List;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TAlterTableOption;
import gudusoft.gsqlparser.nodes.TAlterTableOptionList;
import gudusoft.gsqlparser.nodes.TColumnDefinition;
import gudusoft.gsqlparser.nodes.TColumnDefinitionList;
import gudusoft.gsqlparser.nodes.TColumnWithSortOrder;
import gudusoft.gsqlparser.nodes.TConstraint;
import gudusoft.gsqlparser.nodes.TConstraintList;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TObjectNameList;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TPTNodeList;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TTypeName;
import gudusoft.gsqlparser.stmt.TAlterTableStatement;
import gudusoft.gsqlparser.stmt.TCreateIndexSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TTruncateStatement;

public class AnalyzeStmt {
	private EDbVendor targetType;
	private String tableName;

	public AnalyzeStmt(EDbVendor targetType, String tableName) {
		this.targetType = targetType;
		this.tableName = tableName;
	}

	/**
	 * 分析 sql 段，返回新的 sql 语句
	 * 
	 * @param tableName
	 * @param stmt
	 * @return
	 */
	public List<String> analyzeStmt(TCustomSqlStatement stmt) {
		switch (stmt.sqlstatementtype) {
		case sstcreatetable:
			return createSql((TCreateTableSqlStatement) stmt);
		case sstcreateindex:
			break;
		case sstaltertable:
			return createSql((TAlterTableStatement) stmt);
		case sstcreateview:
			return createSql((TCreateViewSqlStatement) stmt);
		case sstTruncate:
			return createSql((TTruncateStatement) stmt);
		}
		return null;
	}

	/**
	 * 生成 Create Index 段 sql 语句
	 * 
	 * @param stmt
	 * @return
	 */
	private List<String> createSql(TCreateIndexSqlStatement stmt) {
		if (this.tableName == null) {
			this.tableName = stmt.getTableName().toString();
		}
		return createIndex(stmt);
	}

	/**
	 * 创建视图
	 * @param stmt
	 * @return
	 */
	private List<String> createSql(TCreateViewSqlStatement stmt) {
		List<String> list = new ArrayList<>();
		String cols = "";
		TResultColumnList  resultColumnList = stmt.getResultColumnList();
		if(resultColumnList != null) {
			for(int i = 0; i < resultColumnList.size(); i++) {
				TResultColumn column = resultColumnList.getResultColumn(i);
				cols += column.getColumnNameOnly();
				if(i < resultColumnList.size() - 1) {
					cols += ",";
				}
			}
		}
		if(cols.length() > 0) {
			cols = "(" + cols + ")";
		}
		TSelectSqlStatement optionlist = stmt.getSubquery();
		String sql = "CREATE OR REPLACE VIEW " + stmt.getViewName() + cols + " AS " + optionlist.toString();
		list.add(sql);
		return list;
	}
	
	/**
	 * 生成 Create Table 段 sql 语句
	 * 
	 * @param stmt
	 * @return
	 */
	private List<String> createSql(TCreateTableSqlStatement stmt) {
		if (this.tableName == null) {
			this.tableName = stmt.getTableName().toString();
		}
		return createTable(stmt);
	}

	/**
	 * 生成 Alter Table 段 sql 语句
	 * 
	 * @param stmt
	 * @return
	 */
	private List<String> createSql(TAlterTableStatement stmt) {
		List<String> list = new ArrayList<>();
		if (this.tableName == null) {
			this.tableName = stmt.getTableName().toString();
		}
		TAlterTableOptionList optionlist = stmt.getAlterTableOptionList();
		for (int i = 0; i < optionlist.size(); i++) {
			TAlterTableOption option = optionlist.getAlterTableOption(i);
			if (option != null) {
				list.addAll(parserSql(option));
			}
		}
		return list;
	}

	/**
	 * 生成 Truncate Table 段 sql 语句
	 * 
	 * @param stmt
	 * @return
	 */
	private List<String> createSql(TTruncateStatement stmt) {
		List<String> list = new ArrayList<>();
		if (this.tableName == null) {
			this.tableName = stmt.getTableName().toString();
		}
		list.add("TRUNCATE TABLE " + this.tableName);
		return list;
	}

	protected List<String> parserSql(TAlterTableOption ato) {
		List<String> list = new ArrayList<>();
		switch (ato.getOptionType()) {
		case AddColumn:
			list = addCloumn(ato);
			break;
		case ModifyColumn:
			list = modifyCloumn(ato);
			break;
		case AlterColumn:
			list = alterCloumn(ato);
			break;
		case DropColumn:
			// System.out.println(ato.getColumnName().toString());
			list = dropCloumn(ato);
			break;
		case SetUnUsedColumn: // oracle
			break;
		case DropUnUsedColumn:
			break;
		case DropColumnsContinue:
			break;
		case RenameColumn:
			list = renameCloumn(ato);
			break;
		case ChangeColumn: // MySQL
			list = changeColumn(ato);
			break;
		case RenameTable: // MySQL
			// System.out.println(ato.getColumnName().toString());
			list = renameTable(ato);
			break;
		case AddConstraint:
			System.out.println("AddConstraint:" + ato.getColumnName().toString());
			// printConstraintList(ato.getConstraintList());
			break;
		case AddConstraintIndex: // MySQL
			list = addConstraintIndex(ato);
			break;
		case AddConstraintPK:
			list = addConstraintPK(ato);
			break;
		case AddConstraintUnique:
			list = addConstraintUnique(ato);
			break;
		case AddConstraintFK:
			list = addConstraintFK(ato);
			break;
		case ModifyConstraint:
			System.out.println(ato.getConstraintName().toString());
			break;
		case RenameConstraint:
			list = renameConstraint(ato);
			break;
		case DropConstraint:
			System.out.println(ato.getConstraintName().toString());
			break;
		case DropConstraintPK:
			list = dropConstraintPK(ato);
			break;
		case DropConstraintFK:
			list = dropConstraintFK(ato);
			break;
		case DropConstraintCheck: // db2
			System.out.println(ato.getConstraintName());
			break;
		case DropConstraintPartitioningKey:
			break;
		case DropConstraintRestrict:
			break;
		case DropConstraintUnique:
		case DropConstraintIndex:
			list = dropConstraintIndex(ato);
			break;
		case DropConstraintKey:
			list = dropConstraintKey(ato);
			break;
		case AlterConstraintFK:
			System.out.println(ato.getConstraintName());
			break;
		case AlterConstraintCheck:
			System.out.println(ato.getConstraintName());
			break;
		case CheckConstraint:
			break;
		case OraclePhysicalAttrs:
		case toOracleLogClause:
		case OracleTableP:
		case MssqlEnableTrigger:
		case MySQLTableOptons:
		case Db2PartitioningKeyDef:
		case Db2RestrictOnDrop:
		case Db2Misc:
		case Unknown:
			break;
		}
		return list;
	}

	/**
	 * 创建索引
	 * 
	 * @param pStmt
	 * @return
	 */
	private List<String> createIndex(TCreateIndexSqlStatement pStmt) {
		List<String> list = new ArrayList<>();
		TCreateIndexSqlStatement createIndex = pStmt;
		if (this.tableName == null) {
			this.tableName = createIndex.getTableName().toString();
		}
		TOrderByItemList lst = createIndex.getColumnNameList();
		if (lst != null) {
			String cols = "";
			for (int i = 0; i < lst.size(); i++) {
				cols += "\"" + lst.getElement(i).toString().replace("`", "") + "\"";
				if (i < lst.size() - 1) {
					cols += ",";
				}
			}
			String sql = "CREATE INDEX IF NOT EXISTS " + createIndex.getIndexName().toString() + " ON " + tableName
					+ " (" + cols + ")";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 建表
	 * 
	 * @param pStmt
	 * @return
	 */
	private List<String> createTable(TCreateTableSqlStatement pStmt) {
		List<String> list = new ArrayList<>();
		if (this.tableName == null) {
			this.tableName = pStmt.getTargetTable().toString();
		}
		String sql = "CREATE TABLE IF NOT EXISTS " + this.tableName + " (";
		TColumnDefinition column;
		for (int i = 0; i < pStmt.getColumnList().size(); i++) {
			column = pStmt.getColumnList().getColumn(i);
			String dataType = DataTypeUtils.getDataType(this.targetType, column.getDatatype().toString());
			sql += "\"" + column.getColumnName().toString().replace("`", "") + "\" " + dataType;

			if (column.getDefaultExpression() != null) {
				sql += " " + column.getDefaultExpression().toString();
			}

			if (column.getConstraints() != null) {
				for (int j = 0; j < column.getConstraints().size(); j++) {
					sql += printConstraint(column.getConstraints().getConstraint(j), false);
				}
			}

			if (i < pStmt.getColumnList().size() - 1) {
				sql += ",";
			}
		}

		if (pStmt.getTableConstraints().size() > 0) {

			for (int i = 0; i < pStmt.getTableConstraints().size(); i++) {
				TConstraint constraint = pStmt.getTableConstraints().getConstraint(i);
				String s = printConstraint(constraint, true);
				if (s != null && s.length() > 0) {
					sql += ",";
				}
				if (constraint.getKeyActions() != null) {
					sql += " " + constraint.getKeyActions();
				}
			}
		}
		sql += ")";

		list.add(sql);
		if (pStmt.getTableConstraints().size() > 0) {
			for (int i = 0; i < pStmt.getTableConstraints().size(); i++) {
				TConstraint constraint = pStmt.getTableConstraints().getConstraint(i);
				List<String> indexList = indexConstraint(constraint);
				if (indexList.size() > 0) {
					list.addAll(indexList);
				}
			}
		}

		return list;
	}

	private List<String> indexConstraint(TConstraint constraint) {
		List<String> list = new ArrayList<>();
		switch (constraint.getConstraint_type()) {
		case index:
			if (constraint.getColumnList() != null) {

				String cols = "";
				String indexName = tableName;
				for (int i = 0; i < constraint.getColumnList().size(); i++) {
					TColumnWithSortOrder lst = constraint.getColumnList().getElement(i);
					if (lst != null) {
						cols += "\"" + lst.getColumnName().toString().replace("`", "") + "\"";
						indexName += "_" + lst.getColumnName().toString().replace("`", "");
						if (i < constraint.getColumnList().size() - 1) {
							cols += ",";
						}
					}
				}

				String sql = "CREATE INDEX IF NOT EXISTS "
						+ (constraint.getConstraintName() == null ? indexName : constraint.getConstraintName()) + " ON "
						+ tableName + " (" + cols + ")";
				list.add(sql);
			}
			break;

		default:
			break;
		}
		return list;
	}

	private String printConstraint(TConstraint constraint, Boolean outline) {
		String sql = "";
		if (constraint.getConstraintName() != null) {
			System.out.println("\t\tConstraint Name: " + constraint.getConstraintName().toString());
		}
		System.out.println("\t\tConstraint Type:" + constraint.getConstraint_type());
		switch (constraint.getConstraint_type()) {
		case notnull:
			sql = " NOT NULL ";
			break;

		case primary_key:
			sql = " PRIMARY KEY ";
			if (outline) {
				String lcStr = "";
				if (constraint.getColumnList() != null) {
					for (int k = 0; k < constraint.getColumnList().size(); k++) {
						if (k != 0) {
							lcStr = lcStr + ",";
						}
						lcStr = lcStr + constraint.getColumnList().getElement(k).toString().replace("`", "");
					}
					sql += " (" + lcStr + ")";
					System.out.println("\t\tPrimary Key Columns: " + lcStr);
				}
			}
			break;
		case unique:
			sql = " UNIQUE ";
			System.out.println("\t\tUnique key");
			if (outline) {
				String lcStr = "";
				if (constraint.getColumnList() != null) {
					for (int k = 0; k < constraint.getColumnList().size(); k++) {
						if (k != 0) {
							lcStr = lcStr + ",";
						}
						lcStr = lcStr + constraint.getColumnList().getElement(k).toString().replace("`", "");
					}
				}
				sql += "KEY (" + lcStr + ")";
				System.out.println("\t\tColumns: " + lcStr);
			}
			break;
		case check:
			sql += " CHECK (" + constraint.getCheckCondition().toString().replace("`", "") + ")";
			System.out.println("\t\tCheck: " + constraint.getCheckCondition().toString().replace("`", ""));
			break;
		case key:
			// sql += " KEY (" + constraint.getCheckCondition().toString() + ")";
			System.out.println("\t\tKey: " + constraint.toString());
			break;
		case foreign_key:
		case reference:
			System.out.println("\t\tForeign Key");
			if (outline) {
				sql += " CONSTRAINT " + constraint.getConstraintName().toString().replace("`", "") + " FOREIGN KEY ";
				String lcStr = "";
				if (constraint.getColumnList() != null) {
					for (int k = 0; k < constraint.getColumnList().size(); k++) {
						if (k != 0) {
							lcStr = lcStr + ",";
						}
						lcStr = lcStr + constraint.getColumnList().getElement(k).toString().replace("`", "");
					}
				}
				sql += " (" + lcStr + ")";
				System.out.println("\t\tColumns: " + lcStr);
			}

			System.out.println("\t\tReferenced Table: " + constraint.getReferencedObject().toString());

			if (constraint.getReferencedColumnList() != null) {
				sql += " REFERENCES ";
				String lcStr = "";
				for (int k = 0; k < constraint.getReferencedColumnList().size(); k++) {
					if (k != 0) {
						lcStr = lcStr + ",";
					}
					lcStr = lcStr + constraint.getReferencedColumnList().getObjectName(k).toString().replace("`", "");
				}
				sql += constraint.getReferencedObject().toString() + "(" + lcStr + ")";
				System.out.println("\t\tReferenced Columns: " + lcStr);
			}
			break;
		case default_value:
			sql += " DEFAULT " + constraint.getDefaultExpression();
			break;
		default:
			break;

		}
		return sql;
	}

	private List<String> renameCloumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName columnName = option.getColumnName();
		TObjectName newColumnName = option.getNewColumnName();
		if (columnName != null && newColumnName != null) {
			String sql = "ALTER TABLE " + tableName + " RENAME COLUMN \"" + columnName.toString().replace("`", "")
					+ "\" TO \"" + newColumnName.toString().replace("`", "") + "\"";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 添加字段
	 * 
	 * @param option
	 * @return
	 */
	private List<String> addCloumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TColumnDefinitionList colDefList = option.getColumnDefinitionList();
		if (colDefList != null) {
			for (int j = 0; j < colDefList.size(); j++) {
				TColumnDefinition col = colDefList.getColumn(j);
				TObjectName colName = col.getColumnName();
				TTypeName type = col.getDatatype();
				String dataType = DataTypeUtils.getDataType(this.targetType, type.toString());
				TConstraintList constraint = col.getConstraints();
				String sql =  "ALTER TABLE " + this.tableName + " ADD " + colName.toString().replace("`", "") + " "+ dataType;
				if(colName.toString().indexOf(" ") > -1) {
					sql = "ALTER TABLE " + this.tableName + " ADD \"" + colName.toString().replace("`", "") + "\" "+ dataType;
				}
				if (constraint != null) {
					for (int k = 0; k < constraint.size(); k++) {
						TConstraint tc = constraint.getConstraint(k);
						sql += " " + tc.toString();
					}
				}
				list.add(sql);
			}
		}
		return list;
	}

	/**
	 * 删除字段
	 * 
	 * @param option
	 * @return
	 */
	private List<String> dropCloumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TSourceToken colDefList = option.getStartToken();
		if (colDefList != null) {
			if("DROP".equalsIgnoreCase(colDefList.toString())) {
				list.add("ALTER TABLE " + tableName +" "+ option.toString());
			}else {
				String sql = "ALTER TABLE " + tableName + " DROP " + option.getColumnName().toString().replace("`", "")
						+ "";
				if(option.getColumnName().toString().indexOf(" ") > -1) {
					sql = "ALTER TABLE " + tableName + " DROP \"" + option.getColumnName().toString().replace("`", "")
							+ "\"";
				}
				list.add(sql);
			}
		}
		return list;
	}

	/**
	 * 修改字段
	 * 
	 * @param option
	 * @return
	 */
	private List<String> modifyCloumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TColumnDefinitionList colDefList = option.getColumnDefinitionList();
		if (colDefList != null) {
			for (int j = 0; j < colDefList.size(); j++) {
				TColumnDefinition col = colDefList.getColumn(j);
				TObjectName colName = col.getColumnName();
				TTypeName type = col.getDatatype();
				String dataType = DataTypeUtils.getDataType(this.targetType, type.toString());
				String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
						+ "\" TYPE " + dataType;
				list.add(sql);

				TConstraintList constraint = col.getConstraints();
				if (constraint != null) {
					for (int k = 0; k < constraint.size(); k++) {
						sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
								+ "\" SET";
						TConstraint tc = constraint.getConstraint(k);
						sql += " " + tc.toString();
						list.add(sql);
					}
				}
			}
		}
		return list;
	}

	private List<String> alterCloumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName colName = option.getColumnName();
		TColumnDefinitionList defList = option.getColumnDefinitionList();
		TExpression expression = option.getDefaultExpr();

		if (colName != null && expression != null) {
			String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
					+ "\" SET DEFAULT " + expression;
			list.add(sql);
		}
		if (defList != null) {
			for (int i = 0; i < defList.size(); i++) {
				TColumnDefinition defCol = defList.getColumn(i);
				TObjectName name = defCol.getColumnName();
				TTypeName colDatatype = defCol.getDatatype();
				TExpression colExpression = defCol.getDefaultExpression();
				if (colDatatype != null) {
					String dataType = DataTypeUtils.getDataType(this.targetType, colDatatype.toString());
					String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + name.toString().replace("`", "")
							+ "\" TYPE " + dataType;
					list.add(sql);
				}
				if (colExpression != null) {
					String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + name.toString().replace("`", "")
							+ "\" SET DEFAULT " + colExpression;
					list.add(sql);
				}
			}
		}
		if (list.size() == 0) {// 待测试
			String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
					+ "\" DROP DEFAULT";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 改变字段
	 * 
	 * @param option
	 * @return
	 */
	private List<String> changeColumn(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		String colName = option.getColumnName().toString();
		TColumnDefinitionList defList = option.getColumnDefinitionList();
		TColumnDefinition newColumnDef = option.getNewColumnDef();
		if (newColumnDef != null && colName != null) {
			String newColumnName = "";
			String newColumnType = "";
			String[] newcol = newColumnDef.toString().split(" ");
			if (newcol.length == 2) {
				newColumnName = newcol[0].toLowerCase();
				newColumnType = newcol[1];
			}

			String dataType = DataTypeUtils.getDataType(this.targetType, newColumnType);
			String sql = "ALTER TABLE " + tableName + " RENAME \"" + colName.toString().replace("`", "") + "\" TO \""
					+ newColumnName.replace("`", "") + "\"";
			list.add(sql);
			sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + newColumnName.replace("`", "") + "\" TYPE "
					+ dataType;
			list.add(sql);

			TConstraintList constraint = option.getNewColumnDef().getConstraints();
			if (constraint != null) {
				for (int k = 0; k < constraint.size(); k++) {
					sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
							+ "\" SET";
					TConstraint tc = constraint.getConstraint(k);
					sql += " " + tc.toString();
					list.add(sql);
				}
			}
		}
		if (defList != null) {
			for (int i = 0; i < defList.size(); i++) {
				TColumnDefinition defCol = defList.getColumn(i);
				TObjectName name = defCol.getColumnName();
				TTypeName datatype = defCol.getDatatype();
				String dataType = DataTypeUtils.getDataType(this.targetType, datatype.toString());

				String sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + name.toString().replace("`", "")
						+ "\" TYPE " + dataType;
				list.add(sql);

				TConstraintList constraint = defCol.getConstraints();
				if (constraint != null) {
					for (int k = 0; k < constraint.size(); k++) {
						sql = "ALTER TABLE " + tableName + " ALTER COLUMN \"" + colName.toString().replace("`", "")
								+ "\" SET";
						TConstraint tc = constraint.getConstraint(k);
						sql += " " + tc.toString();
						list.add(sql);
					}
				}
			}
		}
		return list;
	}

	/**
	 * 添加索引
	 * 
	 * @param option
	 * @return
	 */
	private List<String> addConstraintIndex(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName indexName = option.getConstraintName();
		TPTNodeList<TColumnWithSortOrder> colList = option.getIndexCols();
		TObjectName columnName = option.getColumnName();
		if (columnName != null) {
			String sql = "CREATE INDEX \"" + indexName.toString().replace("`", "") + "\" ON \"" + tableName + "\" (\""
					+ columnName.toString().replace("`", "") + "\")";
			list.add(sql);
		}

		if (colList != null) {
			String cols = "";
			for (int i = 0; i < colList.size(); i++) {
				cols += "\"" + colList.getElement(i).getColumnName().toString().replace("`", "") + "\"";
				if (i < colList.size() - 1) {
					cols += ",";
				}
			}
			String sql = "CREATE INDEX \"" + indexName.toString().replace("`", "") + "\" ON \"" + tableName + "\" (\""
					+ cols + "\");";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 删除索引
	 * 
	 * @param option
	 * @return
	 */
	private List<String> dropConstraintIndex(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName constraint = option.getConstraintName();
		if (option.getConstraintName() != null) { // db2
			String sql = "DROP INDEX \"" + constraint.toString().replace("`", "") + "\"";
			list.add(sql);
		}

		if (option.getColumnNameList() != null) {// oracle
			TObjectNameList lst = option.getColumnNameList();
			for (int i = 0; i < lst.size(); i++) {
				TObjectName name = lst.getObjectName(i);
				String sql = "DROP INDEX \"" + name.toString().replace("`", "") + "\"";
				list.add(sql);
			}
		}

		return list;
	}

	/**
	 * 添加索引
	 * 
	 * @param option
	 * @return
	 */
	private List<String> addConstraintUnique(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName indexName = option.getConstraintName();
		TPTNodeList<TColumnWithSortOrder> colList = option.getIndexCols();
		TObjectName columnName = option.getColumnName();
		TObjectName constraintName = option.getConstraintName();
		if (columnName != null) {
			String sql = "CREATE UNIQUE INDEX \"" + constraintName.toString().replace("`", "") + "\" ON " + tableName
					+ " (\"" + columnName.toString().replace("`", "") + "\")";
			list.add(sql);
		}

		if (colList != null) {
			String cols = "";
			for (int i = 0; i < colList.size(); i++) {
				cols += "\"" + colList.getElement(i).getColumnName().toString().replace("`", "") + "\"";
				if (i < colList.size() - 1) {
					cols += ",";
				}
			}
			String sql = "CREATE UNIQUE INDEX \"" + indexName.toString().replace("`", "") + "\" ON " + tableName
					+ " (\"" + cols + "\")";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 重命名约束
	 * 
	 * @param option
	 * @return
	 */
	private List<String> renameConstraint(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName constraintName = option.getConstraintName();
		TObjectName newConstraintName = option.getNewConstraintName();
		if (constraintName != null && newConstraintName != null) {
			String sql = "ALTER TABLE " + tableName + " RENAME CONSTRAINT \""
					+ constraintName.toString().replace("`", "") + "\" TO \""
					+ newConstraintName.toString().replace("`", "") + "\"";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 重命名表名
	 * 
	 * @param option
	 * @return
	 */
	private List<String> renameTable(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName newTableName = option.getNewTableName();
		if (newTableName != null) {
			String sql = "ALTER TABLE " + tableName + " RENAME \"" + newTableName.toString().replace("`", "") + "\"";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 添加主键
	 * 
	 * @param option
	 * @return
	 */
	private List<String> addConstraintPK(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectNameList colList = option.getColumnNameList();
		if (colList != null) {
			String cols = "";
			for (int i = 0; i < colList.size(); i++) {
				cols += "\"" + colList.getObjectName(i).toString().replace("`", "") + "\"";
				if (i < colList.size() - 1) {
					cols += ",";
				}
			}
			String sql = "ALTER TABLE " + tableName + " ADD PRIMARY KEY(" + cols + ")";
			list.add(sql);
		}
		return list;
	}

	/**
	 * 删除主键
	 * 
	 * @param option
	 * @return
	 */
	private List<String> dropConstraintPK(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		String sql = "ALTER TABLE " + tableName + " DROP PRIMARY KEY";
		list.add(sql);
		return list;
	}

	/**
	 * 删除Key
	 * 
	 * @param option
	 * @return
	 */
	private List<String> dropConstraintKey(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName constraint = option.getConstraintName();
		String sql = "ALTER TABLE " + tableName + " DROP KEY \"" + constraint.toString().replace("`", "") + "\"";
		list.add(sql);
		return list;
	}

	/**
	 * 添加外键
	 * 
	 * @param option
	 * @return
	 */
	private List<String> addConstraintFK(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectNameList colList = option.getColumnNameList();
		TObjectName refTableName = option.getReferencedObjectName();
		TObjectNameList refList = option.getReferencedColumnList();
		TObjectName constraintName = option.getConstraintName();

		String cols = "";
		if (colList != null) {
			for (int i = 0; i < colList.size(); i++) {
				cols += "\"" + colList.getObjectName(i).toString().replace("`", "")  + "\"";
				if (i < colList.size() - 1) {
					cols += ",";
				}
			}

			if (constraintName != null) {
				String sql = "ALTER TABLE " + tableName + " ADD FOREIGN KEY(" + cols + ") REFERENCES " + refTableName
						+ "(" + constraintName + ")";
				list.add(sql);
			}

			if (refList != null) {
				String refCols = "";
				for (int i = 0; i < refList.size(); i++) {
					refCols += "\"" + refList.getObjectName(i).toString().replace("`", "")  + "\"";
					if (i < refList.size() - 1) {
						refCols += ",";
					}
				}

				String sql = "ALTER TABLE \"" + tableName + "\" ADD FOREIGN KEY(" + cols + ") REFERENCES "
						+ refTableName + " (" + refCols + ")";
				list.add(sql);
			}
		}
		return list;
	}

	/**
	 * 删除外键
	 * 
	 * @param option
	 * @return
	 */
	private List<String> dropConstraintFK(TAlterTableOption option) {
		List<String> list = new ArrayList<>();
		TObjectName constraint = option.getConstraintName();
		String sql = "ALTER TABLE " + tableName + " DROP CONSTRAINT \"" + constraint.toString().replace("`", "")  + "\"";
		list.add(sql);
		return list;
	}

	/**
	 * 截断表
	 * 
	 * @param stmt
	 * @return
	 */
	public List<String> truncateTable(TTruncateStatement stmt) {
		List<String> list = new ArrayList<>();
		list.add("TRUNCATE TABLE " + this.tableName + "");
		return list;
	}
	
	class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
}
