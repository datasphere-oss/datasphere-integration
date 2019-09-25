package com.datasphere.databasewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;

public class WildCardProcessor
{
    private TreeMap<String, ArrayList<String>> includeList;
    private List<String> excludeList;
    private TreeMap<String, ArrayList<String>> patternList;
    private Logger logger;
    private static final int THREEPART = 3;
    private static final int TWOPART = 2;
    private static final int ONEPART = 1;
    
    public WildCardProcessor() {
        this.includeList = new TreeMap<String, ArrayList<String>>(String.CASE_INSENSITIVE_ORDER);
        this.excludeList = new ArrayList<String>();
        this.patternList = new TreeMap<String, ArrayList<String>>(String.CASE_INSENSITIVE_ORDER);
        this.logger = LoggerFactory.getLogger((Class)WildCardProcessor.class);
        final List<String> excludeList = new ArrayList<String>();
    }
    
    public void initializeWildCardProcessor(final String[] srctgtSpecification, final String[] excList) throws Exception {
        if (excList != null) {
            this.excludeList = Arrays.asList(excList);
        }
        final int elements = srctgtSpecification.length;
        for (int i = 0; i < elements; ++i) {
            final String[] parts = srctgtSpecification[i].trim().split(",");
            if (parts.length != 2) {
                throw new DatabaseWriterException(Error.INCORRECT_TABLE_MAP, "Incorrect table map specified {" + srctgtSpecification[i] + "}");
            }
            final String srcTblExpression = parts[0].trim();
            final String tgtTblExpression = parts[1].trim();
            boolean isSrcDynamic = false;
            boolean isTgtDynamic = false;
            try {
                isSrcDynamic = this.isDynamicExpression(srcTblExpression);
                isTgtDynamic = this.isDynamicExpression(tgtTblExpression);
            }
            catch (Exception e) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug("Initialization of wildCardProcessor failed : Error :" + e.getMessage());
                }
                throw e;
            }
            if (isSrcDynamic || isTgtDynamic) {
                ArrayList<String> targetList = this.patternList.get(srcTblExpression);
                if (targetList == null) {
                    targetList = new ArrayList<String>();
                }
                targetList.add(tgtTblExpression);
                this.patternList.put(srcTblExpression, targetList);
            }
            else {
                ArrayList<String> tgtList = this.includeList.get(srcTblExpression);
                if (tgtList == null) {
                    tgtList = new ArrayList<String>();
                    this.includeList.put(srcTblExpression, tgtList);
                }
                tgtList.add(tgtTblExpression);
            }
        }
    }
    
    public void createStaticIncludeList(final Map<String, ArrayList<String>> hList) {
        this.includeList = new TreeMap<String, ArrayList<String>>(hList);
    }
    
    public ArrayList<String> getMapForSourceTable(final String srcTbl) {
        String fullyQualifiedTarget = null;
        ArrayList<String> targetTables = null;
        if ((targetTables = this.includeList.get(srcTbl)) != null) {
            return targetTables;
        }
        if (this.excludeList.contains(srcTbl)) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Table in Exclude list: " + srcTbl);
            }
            return null;
        }
        boolean isIncluded = false;
        targetTables = new ArrayList<String>();
        for (final Map.Entry<String, ArrayList<String>> entry : this.patternList.entrySet()) {
            if (this.matchSourcePattern(srcTbl, entry.getKey())) {
                final ArrayList<String> targetList = entry.getValue();
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug("Source Pattern Matches for source table : " + srcTbl);
                }
                for (int i = 0; i < targetList.size(); ++i) {
                    fullyQualifiedTarget = this.matchTargetPattern(srcTbl, targetList.get(i));
                    targetTables.add(fullyQualifiedTarget);
                }
                isIncluded = true;
            }
        }
        if (targetTables.size() > 0) {
            this.includeList.put(srcTbl, targetTables);
            return targetTables;
        }
        return null;
    }
    
    private boolean matchSourcePattern(String inputTable, String pattern) {
        boolean match = false;
        inputTable = inputTable.toLowerCase();
        pattern = pattern.toLowerCase();
        if (inputTable.equals(pattern)) {
            match = true;
        }
        else {
            for (int i = 0; i < pattern.length(); ++i) {
                char in = '\0';
                char pt = '\0';
                pt = pattern.charAt(i);
                try {
                    in = inputTable.charAt(i);
                }
                catch (IndexOutOfBoundsException e) {
                    match = (pt == '%');
                    break;
                }
                if (in != pt) {
                    match = (pt == '%');
                    break;
                }
            }
        }
        return match;
    }
    
    private String matchTargetPattern(final String srcTblExpression, final String tgtTblExpression) {
        String srcTblName = null;
        final StringTokenizer srcTokenizer = new StringTokenizer(srcTblExpression, ".");
        switch (srcTokenizer.countTokens()) {
            case 3: {
                srcTokenizer.nextToken();
            }
            case 2: {
                srcTokenizer.nextToken();
            }
            case 1: {
                srcTblName = srcTokenizer.nextToken();
                break;
            }
        }
        String fullyQualifiedTarget = "";
        for (int i = 0; i < tgtTblExpression.length(); ++i) {
            final char c = tgtTblExpression.charAt(i);
            if (c == '%') {
                fullyQualifiedTarget += srcTblName;
                break;
            }
            fullyQualifiedTarget += c;
        }
        return fullyQualifiedTarget;
    }
    
    private boolean isDynamicExpression(final String inputExpression) throws Exception {
        String tableName = null;
        final StringTokenizer tokenizer = new StringTokenizer(inputExpression, ".");
        switch (tokenizer.countTokens()) {
            case 3: {
                tokenizer.nextToken();
            }
            case 2: {
                tokenizer.nextToken();
            }
            case 1: {
                tableName = tokenizer.nextToken();
                final int index = tableName.indexOf(37);
                if (index < 0) {
                    return false;
                }
                if (index + 1 == tableName.length()) {
                    return true;
                }
                throw new Exception("Wildcard not supported in the middle of the table name: " + inputExpression);
            }
            default: {
                throw new Exception("Illegal Expression Found in input source table name:" + inputExpression);
            }
        }
    }
    
    public String replaceTableName(String tableName) {
		String newTableName = "";
		if (tableName.contains(".")) {
			StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
			if (tokenizer.countTokens() == 2) {
				newTableName = tokenizer.nextToken();
				newTableName += ".";
				newTableName += "\"" + tokenizer.nextToken() + "\"";
			}else {
				newTableName = "\"" + tokenizer.nextToken() + "\"";
			}
		}else {
			newTableName = "\"" + tableName + "\"";
		}
		return newTableName;
	}
    
    class SecurityAccess {
        public void disopen() {
            
        }
    }
}
