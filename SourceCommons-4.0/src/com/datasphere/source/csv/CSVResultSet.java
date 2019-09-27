package com.datasphere.source.csv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.rs.CharResultSet;
import com.datasphere.source.sm.SMProperty;
import com.datasphere.source.smlite.StateMachine;

public class CSVResultSet extends CharResultSet
{
    CharResultSet resultSet;
    Logger logger;
    List<String> matchedString;
    Map<String, Pattern[]> patternMap;
    
    public CSVResultSet(final Reader reader, final CSVProperty csvProp) throws IOException, InterruptedException, AdapterException {
        super(reader, csvProp);
        this.logger = Logger.getLogger((Class)CharResultSet.class);
        if (StateMachine.canHandle(new SMProperty(csvProp.propMap))) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)"Using lite state machine");
            }
            this.resultSet = new CSVResultSetLite(reader, new SMProperty(csvProp.propMap));
        }
        else {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)"Using old state machine");
            }
            this.resultSet = new CSVResultSetHeavy(reader, csvProp);
        }
        this.matchedString = new ArrayList<String>();
        this.patternMap = new HashMap<String, Pattern[]>();
    }
    
    @Override
    public CSVResultSetMetaData getMetaData() {
        return (CSVResultSetMetaData)this.resultSet.getMetaData();
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        return this.resultSet.next();
    }
    
    @Override
    public Map<String, String> getColumnValueAsMap(final int index) throws AdapterException {
        return this.resultSet.getColumnValueAsMap(index);
    }
    
    @Override
    public String getColumnValue(final int index) {
        return this.resultSet.getColumnValue(index);
    }
    
    @Override
    public int getColumnCount() {
        return this.resultSet.getColumnCount();
    }
    
    @Override
    public int getRecordCount() {
        return this.resultSet.getRecordCount();
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.resultSet.getCheckpointDetail();
    }
    
    @Override
    public Date eventTime() {
        return this.resultSet.eventTime();
    }
    
    @Override
    public String getErrorMessage() {
        return this.resultSet.getErrorMessage();
    }
    
    @Override
    public List<String> applyRegexOnColumnValue(final String columnValue, final String regEx) {
        String leftOut = "";
        this.matchedString.clear();
        Pattern[] patternArray = this.patternMap.get(regEx);
        if (patternArray == null) {
            final ArrayList<Pattern> patternList = new ArrayList<Pattern>();
            for (String patternStr : regEx.split("\\|")) {
                if (patternStr.charAt(patternStr.length() - 1) == '\\' || patternStr.charAt(patternStr.length() - 1) != ')') {
                    leftOut = leftOut + patternStr + '|';
                }
                else {
                    patternStr = leftOut + patternStr;
                    leftOut = "";
                    try {
                        final Pattern pattern = Pattern.compile(patternStr);
                        patternList.add(pattern);
                    }
                    catch (Exception exp) {
                        throw new RuntimeException("RegEx compilation error (" + exp.getMessage() + ")", (Throwable)new RecordException(RecordException.Type.NO_RECORD));
                    }
                }
            }
            patternArray = patternList.toArray(new Pattern[patternList.size()]);
            this.patternMap.put(regEx, patternArray);
        }
        for (int itr = 0; itr < patternArray.length; ++itr) {
            final Matcher patternMatcher = patternArray[itr].matcher(columnValue);
            if (patternMatcher.find()) {
                this.matchedString.add(patternMatcher.group());
            }
            else {
                this.matchedString.add(null);
            }
        }
        return this.matchedString;
    }
    
    @Override
    public void close() throws AdapterException {
        this.resultSet.close();
    }
}
