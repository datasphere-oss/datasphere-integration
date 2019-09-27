package com.datasphere.Tables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.Exceptions.ColumnMapException;
import com.datasphere.Exceptions.Error;
import com.datasphere.Exceptions.ImproperConfig;
import com.datasphere.Fetchers.Fetcher;
import com.datasphere.Fetchers.FetcherFactory;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.WildCardProcessor;

public class ColumnMap
{
    private Logger logger;
    public static String COLMAP_MARKER;
    Map<String, Map<String, String>> tableToColMap;
    WildCardProcessor wildcardProcessor;
    
    public ColumnMap(final Property prop, final WildCardProcessor wld) throws ImproperConfig {
        this.logger = Logger.getLogger((Class)ColumnMap.class);
        this.tableToColMap = this.initializeColMapDetails(prop);
        this.wildcardProcessor = wld;
    }
    
    private Map<String, Map<String, String>> initializeColMapDetails(final Property prop) throws ImproperConfig {
        final String tables = prop.getString(Constant.TABLES_PROPETY, null);
        final Map<String, Object> mapProperties = getMapProperties(tables);
        final Map<String, Map<String, String>> colMap = new TreeMap<String, Map<String, String>>(String.CASE_INSENSITIVE_ORDER);
        final ArrayList<String> sourceLst = (ArrayList<String>)mapProperties.get("SOURCE_LIST");
        for (int itr = 0; itr < sourceLst.size(); ++itr) {
            final String colMapKey = sourceLst.get(itr) + "-" + ColumnMap.COLMAP_MARKER;
            final Map<String, String> tmpColMap = (Map<String, String>)mapProperties.get(colMapKey);
            colMap.put(sourceLst.get(itr), tmpColMap);
        }
        return colMap;
    }
    
    public static Map<String, Object> getMapProperties(final String property) throws ImproperConfig {
        final Map<String, Object> mapInfo = new HashMap<String, Object>();
        final String[] tblMapProps = property.split(";");
        final ArrayList<String> srcArrayLst = new ArrayList<String>();
        String srcTbl = "";
        String tgtTbl = "";
        for (int itr = 0; itr < tblMapProps.length; ++itr) {
            final Pattern tblNamePtrn = Pattern.compile("([^\\s]+)");
            final String regEx = "(?i)(?<=[\\s\\S\\t\\r]" + ColumnMap.COLMAP_MARKER + "\\().*(?=\\))";
            final Pattern colMapPtrn = Pattern.compile(regEx);
            boolean hasAdditionalProperties = false;
            tblMapProps[itr] = tblMapProps[itr].trim();
            final String tableProps = tblMapProps[itr];
            final Matcher tblMatcher = tblNamePtrn.matcher(tableProps);
            if (!tblMatcher.find()) {
                throw new ImproperConfig("Table mapping is not specified correctly {" + tblMapProps[itr] + "}");
            }
            final String tableParts = tblMatcher.group();
            if (tableParts != null) {
                final String[] tbls = tableParts.split(",");
                if (tbls.length != 2 || tableParts.indexOf(40) != -1) {
                    throw new ImproperConfig("Table mapping is not specified correctly {" + tblMapProps[itr] + "}");
                }
                srcTbl = tbls[0];
                tgtTbl = tbls[1];
                mapInfo.put(srcTbl + "-TARGET", tgtTbl);
            }
            tblMapProps[itr] = tblMapProps[itr].replace("\n", "").replace("\r", "");
            final Matcher colMapMatcher = colMapPtrn.matcher(tblMapProps[itr]);
            if (colMapMatcher.find()) {
                hasAdditionalProperties = true;
                final String colMapPorps = colMapMatcher.group();
                if (colMapPorps.trim().isEmpty()) {
                    throw new ImproperConfig("Empty column map is not allowed {" + tableProps + "}");
                }
                final Map<String, String> colMap = intiailizeColMap(colMapPorps);
                mapInfo.put(srcTbl + "-" + tgtTbl + "-" + ColumnMap.COLMAP_MARKER, colMap);
            }
            else {
                final int colMapIdx = tblMapProps[itr].toUpperCase().indexOf(ColumnMap.COLMAP_MARKER);
                if (colMapIdx != -1) {
                    throw new ImproperConfig("Couldn't parse colmap options properly for {" + tableProps + "}, please specify colmap property as '" + ColumnMap.COLMAP_MARKER + "(...)'");
                }
            }
            if (!hasAdditionalProperties) {
                final String extraProp = tblMapProps[itr].substring(tableParts.length(), tblMapProps[itr].length());
                if (!extraProp.trim().isEmpty()) {
                    throw new ImproperConfig("Unrecoganized property specified {" + extraProp.trim() + "}");
                }
            }
            srcArrayLst.add(srcTbl + "-" + tgtTbl);
        }
        if (!srcArrayLst.isEmpty()) {
            mapInfo.put("SOURCE_LIST", srcArrayLst);
        }
        return mapInfo;
    }
    
    public static String extractColMapProperty(final String mapProp) {
        final String regEx = "(?i)(?<= " + ColumnMap.COLMAP_MARKER + "\\().+(?=\\))";
        final Pattern colMapPtrn = Pattern.compile(regEx);
        final Matcher patternMatcher = colMapPtrn.matcher(mapProp);
        if (!patternMatcher.find()) {
            return null;
        }
        final String prop = patternMatcher.group();
        if (prop.trim().isEmpty()) {
            return null;
        }
        return prop;
    }
    
    private static Map<String, String> intiailizeColMap(final String mapInfo) throws ImproperConfig {
        final Map<String, String> mapDetails = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        final String[] individualColMapInfo = mapInfo.split(",");
        for (int itr = 0; itr < individualColMapInfo.length; ++itr) {
            final String[] colDetails = individualColMapInfo[itr].split("=");
            if (colDetails.length != 2) {
                throw new ImproperConfig("Incorrect column mapping is specified in {" + mapInfo + "}");
            }
            mapDetails.put(colDetails[0].trim(), colDetails[1].trim());
        }
        return mapDetails;
    }
    
    private void verifyTargetColumns(final String tableName, final DatabaseColumn[] tgtCols, final Map<String, String> colMap) throws ColumnMapException {
        final Map<String, String> tmpColMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        tmpColMap.putAll(colMap);
        for (int itr = 0; itr < tgtCols.length; ++itr) {
            if (tmpColMap.containsKey(tgtCols[itr].getName())) {
                tmpColMap.remove(tgtCols[itr].getName());
            }
        }
        if (!tmpColMap.isEmpty()) {
            String errMsg = "";
            String cols = "";
            for (final Map.Entry<String, String> entry : tmpColMap.entrySet()) {
                if (cols.isEmpty()) {
                    cols = entry.getKey();
                }
                else {
                    cols = cols + ", " + entry.getKey();
                }
            }
            errMsg = "Target Column(s) {" + cols + "} specified in column mapping does not exists in {" + tableName + "}";
            this.logger.error((Object)errMsg);
            throw new ColumnMapException(Error.MAPPED_COLUMN_DOES_NOT_EXISTS, errMsg);
        }
    }
    
    public Table getMappedTable(final Table srcTbl, final Table tgtTbl) throws ColumnMapException {
        final Table mappedTable = null;
        final StringBuilder mapInfo = new StringBuilder();
        final String tgtFQN = tgtTbl.getFullyQualifiedName();
        final String tgtName = tgtTbl.getName();
        String srcTableName = this.wildcardProcessor.getSourcePattern(tgtTbl.getFullyQualifiedName());
        if (srcTableName == null) {
            srcTableName = srcTbl.getName();
        }
        String tgtTableName = this.wildcardProcessor.getTargetPattern(tgtTbl.getFullyQualifiedName());
        if (tgtTableName == null) {
            tgtTableName = tgtTbl.getName();
        }
        String colMapKey = srcTableName + "-" + tgtTableName;
        Map<String, String> colMap = this.tableToColMap.get(colMapKey);
        if (colMap == null) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Did not find appropriate mapping for fully qualified name {" + colMapKey + "}, going to try with minimal qualified name"));
            }
            final String schema = tgtTbl.getOwner();
            String tableName = tgtTbl.getName();
            if (schema != null) {
                tableName = schema + "." + tableName;
            }
            colMapKey = srcTableName + "-" + tableName;
            colMap = this.tableToColMap.get(colMapKey);
            if (colMap == null) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Did not find appropriate mapping for partially qualified name {" + colMapKey + "}, going to try with table name"));
                }
                colMapKey = srcTableName + "-" + tgtTbl.getName();
                colMap = this.tableToColMap.get(colMapKey);
            }
        }
        Fetcher fetcher = null;
        final DatabaseColumn[] tgtColumns = tgtTbl.getColumns();
        final LinkedList<DBWriterColumn> mappedColumnList = new LinkedList<DBWriterColumn>();
        if (colMap != null) {
            mapInfo.append("\nTrying to map {" + tgtTbl.getFullyQualifiedName() + "} => {" + srcTbl.getName() + "}\n");
            this.verifyTargetColumns(tgtTbl.getFullyQualifiedName(), tgtColumns, colMap);
            final Map<String, DatabaseColumn> srcTblColumns = srcTbl.columnsAsMap();
            for (int itr = 0; itr < tgtColumns.length; ++itr) {
                boolean inColMap = true;
                boolean hasMatchedColumn = false;
                String sourceColumnName = colMap.get(tgtColumns[itr].getName());
                if (sourceColumnName == null) {
                    sourceColumnName = tgtColumns[itr].getName();
                    inColMap = false;
                }
                final DatabaseColumn sourceColumn = srcTblColumns.get(sourceColumnName);
                final DBWriterColumn newCol = new DBWriterColumn((DBWriterColumn)tgtColumns[itr]);
                if (sourceColumn != null) {
                    hasMatchedColumn = true;
                    fetcher = FetcherFactory.createFetcher(sourceColumn.getIndex());
                    newCol.setIndex(sourceColumn.getIndex());
                }
                else if (FetcherFactory.getFetcherType(sourceColumnName) != FetcherFactory.FetcherType.UNKNOWN) {
                    fetcher = FetcherFactory.createFetcher(sourceColumnName);
                    newCol.setIndex(-1);
                }
                else {
                    if (!inColMap) {
                        mapInfo.append("Target Column {" + sourceColumnName + "} => Source Column {null} - Not specified in Colmap and did not find name match in source column list, so it will be ignored\n");
                        this.logger.info((Object)("Target Column {" + sourceColumnName + "} not in source table, so it will be ignored"));
                        continue;
                    }
                    final String errMsg = "Mapped source column {" + sourceColumnName + "} does not exists in {" + srcTableName + "}. Stopping the appication";
                    this.logger.error((Object)errMsg);
                    throw new ColumnMapException(Error.MAPPED_COLUMN_DOES_NOT_EXISTS, errMsg);
                }
                if (inColMap) {
                    if (hasMatchedColumn) {
                        mapInfo.append("Target Column {" + newCol.getName() + "} => Source Column {" + sourceColumn.getName() + "} mapped based ColMap entry\n");
                    }
                    else {
                        mapInfo.append("Target Column {" + newCol.getName() + "} => Source Column {" + ((sourceColumn != null) ? sourceColumn.getName() : "null") + "} Fetch from " + fetcher.toString() + "\n");
                    }
                }
                else {
                    mapInfo.append("Target Column {" + newCol.getName() + "} => Source Column {" + sourceColumn.getName() + "} mapped based on column name\n");
                }
                newCol.setFetcher(fetcher);
                mappedColumnList.add(newCol);
            }
        }
        else {
            this.logger.info((Object)("Following column offset based mapping for {" + tgtTbl.getFullyQualifiedName() + "}"));
            final DatabaseColumn[] srcColumns = srcTbl.getColumns();
            if (srcColumns.length > tgtColumns.length) {
                final String errMsg2 = "Target {" + tgtTbl.getName() + "} has less number of column as compared to source {" + srcTbl.getName() + "}.No of columns in target {" + tgtColumns.length + "} but expected {" + srcColumns.length + "}";
                throw new ColumnMapException(Error.INCONSISTENT_TABLE_STRUCTURE, errMsg2);
            }
            for (int itr = 0; itr < srcColumns.length; ++itr) {
                fetcher = FetcherFactory.createFetcher(itr);
                final DBWriterColumn newCol2 = new DBWriterColumn((DBWriterColumn)tgtColumns[itr]);
                newCol2.setFetcher(fetcher);
                mappedColumnList.add(newCol2);
            }
        }
        final DBWriterColumn[] cols = new DBWriterColumn[mappedColumnList.size()];
        for (int itr = 0; itr < mappedColumnList.size(); ++itr) {
            cols[itr] = mappedColumnList.get(itr);
        }
        tgtTbl.setColumns(cols);
        if (this.logger.isInfoEnabled()) {
            this.logger.info((Object)mapInfo.toString());
        }
        return tgtTbl;
    }
    
    static {
        ColumnMap.COLMAP_MARKER = "COLUMNMAP";
    }
}
