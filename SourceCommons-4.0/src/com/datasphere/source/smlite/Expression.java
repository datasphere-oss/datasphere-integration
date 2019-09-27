package com.datasphere.source.smlite;

import org.apache.log4j.*;

public class Expression
{
    static int expressionCount;
    CharParser parser;
    protected short eventType;
    protected SMEvent eventObject;
    protected String[] pattern;
    Logger logger;
    
    public Expression(final CharParser parser, final String[] pattern, final SMEvent event) {
        this.logger = Logger.getLogger((Class)Expression.class);
        this.pattern = pattern;
        this.parser = parser;
        this.eventObject = event;
    }
    
    public void init() {
        this.initializeHashLookup();
        this.initializeGroupLookup();
    }
    
    public boolean evaluate(final int hashCode) {
        return false;
    }
    
    protected void initializeHashLookup() {
        for (int itr = 0; itr < this.pattern.length; ++itr) {
            if (this.pattern[itr] != null) {
                for (int charItr = 0; charItr < this.pattern[itr].length(); ++charItr) {
                    this.parser.addIntoLookupTable(this.pattern[itr].charAt(charItr));
                }
            }
            else {
                final StringBuilder tmpBuilder = new StringBuilder();
                for (int pItr = 0; pItr < this.pattern.length; ++pItr) {
                    tmpBuilder.append(", " + this.pattern[itr]);
                }
                final String tmp = tmpBuilder.toString();
                this.logger.warn((Object)("Failure in event type {" + this.eventObject.eventState + "} initialization due to invalid pattern : {" + tmp + "}"));
            }
        }
    }
    
    public int initializeGroupLookup() {
        return this.initializeGroupLookup(this.pattern, this.eventObject);
    }
    
    public int initializeGroupLookup(final String delimiter, final SMEvent event) {
        int gId = 0;
        if (delimiter.length() >= 3) {
            String group = "";
            for (int charItr = 0; charItr < delimiter.length() - 1; ++charItr) {
                if (gId == 0) {
                    group = "" + delimiter.charAt(charItr);
                    group = group + "" + delimiter.charAt(charItr + 1);
                    gId = this.parser.addIntoGroupTable(group);
                }
                else if (charItr == delimiter.length() - 2) {
                    this.parser.setEventForGroup(gId, delimiter.charAt(charItr + 1), event);
                }
                else {
                    gId = this.parser.addIntoGroupTable(gId, delimiter.charAt(charItr + 1));
                }
            }
        }
        else if (delimiter.length() == 2) {
            final int tmpgId = this.parser.addIntoLookupTable(delimiter.charAt(0));
            this.parser.setEventForGroup(tmpgId, delimiter.charAt(1), event);
        }
        else if (delimiter.length() == 1) {
            this.parser.setEventForGroup(gId, delimiter.charAt(0), event);
        }
        else if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Initialization called for empty delimiter...");
        }
        return gId;
    }
    
    public int initializeGroupLookup(final String[] delPatter, final SMEvent event) {
        for (int itr = 0; itr < delPatter.length; ++itr) {
            if (delPatter[itr] != null) {
                SMEvent tmpEventObj = null;
                try {
                    tmpEventObj = (SMEvent)event.clone();
                }
                catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
                tmpEventObj.delimiter(delPatter[itr]);
                this.initializeGroupLookup(delPatter[itr], tmpEventObj);
            }
            else {
                final StringBuilder tmpBuilder = new StringBuilder();
                for (int i = 0; i < delPatter.length; ++i) {
                    tmpBuilder.append(", " + delPatter[i]);
                }
                final String tmp = tmpBuilder.toString();
                this.logger.warn((Object)("Failure in getting valid pattern. Pattern list is supposed to be empty but it has {" + tmp + "}"));
            }
        }
        return 0;
    }
    
    public String endOfDelimitChars() {
        return "";
    }
    
    public String interestedChars() {
        return "";
    }
    
    public long compressHash(final long hash) {
        return hash;
    }
    
    public int noOfSpecialChars() {
        return 0;
    }
    
    public char eventDelimiter(final short eventType) {
        return '\0';
    }
    
    public String[] getDelimiters(final short eventId) {
        return null;
    }
    
    public int length() {
        return 0;
    }
    
    public String[] additionalDelimiters() {
        return null;
    }
    
    public String[] getDelimter() {
        return this.pattern;
    }
}
