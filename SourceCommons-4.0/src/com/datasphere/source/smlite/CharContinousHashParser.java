package com.datasphere.source.smlite;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.common.exc.*;

public class CharContinousHashParser extends CharParser
{
    private static Object lock;
    Logger logger;
    final short END_OF_DELIMITER = 16384;
    final short BIT_MASK = 255;
    static final char CHAR_TO_REMOVE_PATTERN = '^';
    String lookupKey;
    static Map<String, LookupReference> lookupMap;
    LookupReference reference;
    SMEvent[] localEventLookupTable;
    short lastCharCode;
    int hashValue;
    short lastCharIdx;
    short[] localHashLookup;
    short[] defaultHashLookup;
    short eventLookupIdx;
    List<String> entryList;
    List<Expression> listOfExpressions;
    List<String> mustOverride;
    Map<String, short[]> ignoreEventCache;
    Map<String, String> otherEventDelimiterCache;
    SMEvent event;
    short hashCode;
    int groupCode;
    SMEvent localEvent;
    
    public CharContinousHashParser(final StateMachine sm, final SMProperty prop) {
        super(sm, prop);
        this.logger = Logger.getLogger((Class)CharContinousHashParser.class);
        this.lookupKey = "";
    }
    
    @Override
    protected synchronized void init() {
        this.hashValue = 0;
        this.lastCharIdx = 0;
        this.entryList = new ArrayList<String>();
        this.listOfExpressions = new ArrayList<Expression>();
        this.mustOverride = new ArrayList<String>();
        this.ignoreEventCache = new HashMap<String, short[]>();
        this.otherEventDelimiterCache = new HashMap<String, String>();
        super.init();
    }
    
    public void initializeDelimiter(final String[] delimters, final short eventType) {
        if (delimters != null) {
            super.initializeDelimiter(delimters, eventType);
            for (int delItr = 0; delItr < delimters.length && delimters[delItr].length() > 0; ++delItr) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Initializging Delimiter[" + delimters[delItr] + "]"));
                }
                this.lookupKey += delimters[delItr];
                final SMEvent eventObj = EventFactory.createEvent(eventType);
                String delimiter = delimters[delItr];
                if (delimiter.length() > 0 && delimiter.charAt(0) == '^' && (eventType == 8 || eventType == 11)) {
                    eventObj.removePattern = true;
                    delimiter = delimiter.substring(1, delimiter.length());
                }
                if (eventObj.state() == 2) {
                    ((RowEvent)eventObj).array(this.stateMachine.buffer());
                }
                eventObj.delimiter(delimiter);
                final Expression exp = ExpressionBuilder.buildExpression(this, delimiter, eventObj);
                final String[] additionalDel = exp.additionalDelimiters();
                if (additionalDel != null) {
                    for (int itr = 0; itr < additionalDel.length; ++itr) {
                        this.mustOverride.add(additionalDel[itr]);
                    }
                }
                this.listOfExpressions.add(exp);
                final String[] tmp = exp.getDelimter();
                for (int itr2 = 0; itr2 < tmp.length; ++itr2) {
                    this.delimiterEventMap.put(tmp[itr2], eventObj);
                }
            }
        }
    }
    
    @Override
    public void finalizeDelimiterInit() {
        synchronized (CharContinousHashParser.lock) {
            boolean needInitialization = false;
            if (CharContinousHashParser.lookupMap == null) {
                CharContinousHashParser.lookupMap = new TreeMap<String, LookupReference>();
            }
            if ((this.reference = CharContinousHashParser.lookupMap.get(this.lookupKey)) == null) {
                this.reference = new LookupReference();
                CharContinousHashParser.lookupMap.put(this.lookupKey, this.reference);
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Adding lookup table for {" + this.lookupKey + "}"));
                }
                needInitialization = true;
            }
            if (needInitialization) {
                super.finalizeDelimiterInit();
                for (int expItr = 0; expItr < this.listOfExpressions.size(); ++expItr) {
                    final Expression expp = this.listOfExpressions.get(expItr);
                    expp.initializeHashLookup();
                }
                this.reference.BIT_SHIFT = this.bitCount(this.lastCharCode);
                for (int expItr = 0; expItr < this.listOfExpressions.size(); ++expItr) {
                    final Expression expp = this.listOfExpressions.get(expItr);
                    expp.initializeGroupLookup();
                }
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Last unique code generated : [" + this.lastCharCode + "]"));
                }
            }
            this.defaultHashLookup = this.reference.hashLookup;
            this.localHashLookup = new short[65536];
            System.arraycopy(this.reference.hashLookup, 0, this.localHashLookup, 0, this.reference.hashLookup.length);
            this.localEventLookupTable = new SMEvent[SMEvent.staticEventIdx + 1];
            ++this.reference.instanceCount;
        }
    }
    
    @Override
    public int addIntoLookupTable(final char c) {
        if (this.reference.hashLookup[(short)c] == 0) {
            this.reference.hashLookup[(short)c] = (short)(++this.lastCharCode);
            this.reference.ordinalHash[this.lastCharCode] = c;
            this.entryList.add("" + c);
            return this.lastCharCode;
        }
        return this.reference.hashLookup[(short)c];
    }
    
    @Override
    public int addIntoLookupTable(final char c, final int ordinal) {
        if (this.reference.ordinalHash[ordinal] != '\0') {
            this.reference.hashLookup[(short)c] = (short)ordinal;
        }
        else {
            this.reference.ordinalHash[ordinal] = c;
            this.reference.hashLookup[(short)c] = (short)ordinal;
        }
        return ordinal;
    }
    
    public String hashToString(final int hashValue) {
        String groupStr = "";
        if (this.reference.groupToHash[hashValue >> this.reference.BIT_SHIFT] != 0) {
            groupStr = this.hashToString(hashValue >> this.reference.BIT_SHIFT);
        }
        else {
            groupStr = groupStr + "" + this.reference.ordinalHash[hashValue >> this.reference.BIT_SHIFT];
        }
        return groupStr;
    }
    
    public String hashToString() {
        String groupStr = "";
        if (this.reference.groupToHash[this.hashValue >> this.reference.BIT_SHIFT] != 0) {
            groupStr = this.hashToString(this.reference.groupToHash[this.hashValue >> this.reference.BIT_SHIFT]);
        }
        else if (this.hashValue >> this.reference.BIT_SHIFT != 0) {
            groupStr = "" + this.reference.ordinalHash[(short)(this.hashValue >> this.reference.BIT_SHIFT)];
        }
        if (this.reference.groupToHash[this.hashValue & 0xFF] != 0) {
            return this.hashToString(this.reference.groupToHash[this.hashValue & 0xFF]);
        }
        groupStr = groupStr + "" + this.reference.ordinalHash[this.hashValue & 0xFF];
        return groupStr;
    }
    
    @Override
    public void setEventForGroup(final int gId, final char c, final SMEvent event) {
        int hash = gId << this.reference.BIT_SHIFT;
        hash |= this.reference.hashLookup[c];
        this.reference.eventLookup[hash] = event;
    }
    
    public void setEventForGroup(final int gId, final SMEvent event) {
        if (this.reference.groupHash[gId] != 0) {
            System.out.println("GroupID [" + gId + "] already has event");
            return;
        }
        final short[] groupHash = this.reference.groupHash;
        final short n = 16384;
        final short eventLookupIdx = (short)(this.eventLookupIdx + 1);
        this.eventLookupIdx = eventLookupIdx;
        groupHash[gId] = (short)(n | eventLookupIdx);
        this.reference.eventLookup[0x4000 | this.eventLookupIdx] = event;
    }
    
    @Override
    public int addIntoGroupTable(int groupId, final char c) {
        String groupStr = "";
        if (groupId != 0) {
            final int hash = this.reference.groupToHash[groupId];
            if (hash == 0) {
                System.out.println("Invalid group id.");
                return 0;
            }
            groupStr = this.hashToString(hash);
            groupStr = groupStr + "" + c;
        }
        else {
            groupStr = "" + c;
        }
        groupId <<= this.reference.BIT_SHIFT;
        groupId |= this.reference.hashLookup[(short)c];
        if (this.reference.groupHash[groupId] != 0) {
            return this.reference.groupHash[groupId];
        }
        this.reference.groupHash[groupId] = (short)(++this.lastCharCode);
        this.reference.groupToHash[this.lastCharCode] = groupId;
        this.entryList.add(groupStr);
        return this.lastCharCode;
    }
    
    @Override
    public int addIntoGroupTable(final String str) {
        final int hashValue = this.computeHashFor(str);
        this.entryList.add(str);
        if (this.reference.groupHash[hashValue] == 0) {
            this.reference.groupHash[hashValue] = (short)(++this.lastCharCode);
            this.reference.groupToHash[this.lastCharCode] = hashValue;
            return this.lastCharCode;
        }
        return this.reference.groupHash[hashValue];
    }
    
    private short bitCount(final int uniqueCode) {
        short cnt = 0;
        for (int bit = 1; bit <= uniqueCode; bit <<= 1, ++cnt) {}
        return cnt;
    }
    
    public int addIntoGroupTable(final String str, final int gId) {
        final int hashValue = this.computeHashFor(str);
        this.entryList.add(str);
        if (this.reference.groupHash[hashValue] == 0) {
            this.reference.groupHash[hashValue] = (short)gId;
        }
        else if (this.reference.groupHash[hashValue] != gId) {
            this.logger.warn((Object)("Seen clash with other group [" + str + "] GID [" + gId + "] existing GID [" + this.reference.groupHash[hashValue] + "]"));
        }
        return gId;
    }
    
    @Override
    public void ignoreEvents(final short[] events, final SMEvent excludeEvent) {
        this.ignoreEvents(events);
        if (excludeEvent != null) {
            String eventString = "" + Arrays.hashCode(events);
            String additionalDelString = "";
            additionalDelString = additionalDelString + "" + excludeEvent.delimiter;
            eventString += additionalDelString;
            short[] tmpHash = this.ignoreEventCache.get(eventString);
            if (tmpHash == null) {
                tmpHash = new short[65536];
                System.arraycopy(this.localHashLookup, 0, tmpHash, 0, this.localHashLookup.length);
                final String delimiter = excludeEvent.delimiter;
                for (int len = 0; len < delimiter.length(); ++len) {
                    if (delimiter.charAt(len) != '\0') {
                        tmpHash[delimiter.charAt(len)] = this.reference.hashLookup[delimiter.charAt(len)];
                    }
                    else {
                        this.logger.warn((Object)"Got NULL character in delimiter");
                    }
                }
                this.ignoreEventCache.put(eventString, tmpHash);
            }
            this.localHashLookup = tmpHash;
        }
    }
    
    @Override
    public void ignoreEvents(final short[] events) {
        if (events == null) {
            this.localHashLookup = this.defaultHashLookup;
        }
        else {
            final String eventString = "" + Arrays.hashCode(events);
            short[] tmpHash = this.ignoreEventCache.get(eventString);
            if (tmpHash == null) {
                tmpHash = new short[65536];
                System.arraycopy(this.defaultHashLookup, 0, tmpHash, 0, this.defaultHashLookup.length);
                String otherEventDelimiterString = this.otherEventDelimiterCache.get(eventString);
                if (otherEventDelimiterString == null) {
                    final StringBuilder otherEventDelimiterStringBuilder = new StringBuilder();
                    for (int stateItr = 0; stateItr < 13; ++stateItr) {
                        boolean skip = false;
                        for (int intItr = 0; intItr < events.length; ++intItr) {
                            if (events[intItr] == stateItr) {
                                skip = true;
                                break;
                            }
                        }
                        if (!skip && this.delimiterList[stateItr] != null) {
                            for (int delItr = 0; delItr < this.delimiterList[stateItr].length; ++delItr) {
                                otherEventDelimiterStringBuilder.append(this.delimiterList[stateItr][delItr]);
                            }
                        }
                    }
                    otherEventDelimiterString = otherEventDelimiterStringBuilder.toString();
                    this.otherEventDelimiterCache.put(eventString, otherEventDelimiterString);
                }
                final StringBuilder eventDelimitersBuilder = new StringBuilder();
                for (int stateItr = 0; stateItr < events.length; ++stateItr) {
                    if (this.delimiterList[events[stateItr]] != null) {
                        for (int delItr2 = 0; delItr2 < this.delimiterList[events[stateItr]].length; ++delItr2) {
                            eventDelimitersBuilder.append(this.delimiterList[events[stateItr]][delItr2]);
                        }
                    }
                }
                final String eventDelimiters = eventDelimitersBuilder.toString();
                for (int itr = 0; itr < eventDelimiters.length(); ++itr) {
                    if (otherEventDelimiterString.indexOf(eventDelimiters.charAt(itr)) == -1) {
                        tmpHash[eventDelimiters.charAt(itr)] = 0;
                    }
                }
                this.ignoreEventCache.put(eventString, tmpHash);
            }
            this.localHashLookup = tmpHash;
        }
    }
    
    @Override
    public int computeHashFor(final String str) {
        int hash = 0;
        for (int itr = 0; itr < str.length(); ++itr) {
            hash <<= this.reference.BIT_SHIFT;
            if (this.reference.hashLookup[str.charAt(itr)] == 0) {
                hash |= this.addIntoLookupTable(str.charAt(itr));
            }
            else {
                hash |= this.reference.hashLookup[str.charAt(itr)];
            }
        }
        return hash;
    }
    
    protected boolean evaluateChar(final char charIdx) {
        final short hashCode = this.localHashLookup[charIdx];
        this.hashCode = hashCode;
        if (hashCode != 0) {
            this.hashValue <<= this.reference.BIT_SHIFT;
            this.hashValue |= this.hashCode;
            final SMEvent event = this.reference.eventLookup[this.hashValue];
            this.event = event;
            if (event != null || (this.event = this.reference.eventLookup[this.hashCode]) != null) {
                this.localEvent = this.localEventLookupTable[this.event.eventIdx];
                if (this.localEvent == null) {
                    try {
                        this.localEvent = (SMEvent)this.event.clone();
                    }
                    catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                    this.localEventLookupTable[this.event.eventIdx] = this.localEvent;
                }
                this.hashValue = 0;
                this.stateMachine.publishEvent(this.localEvent);
                if (this.stateMachine.canBreak()) {
                    return true;
                }
            }
            else if ((this.groupCode = this.reference.groupHash[this.hashValue]) != 0) {
                this.lastCharIdx = 0;
                this.hashValue = this.groupCode;
            }
            else {
                this.hashValue = this.hashCode;
            }
        }
        else {
            this.hashValue = 0;
        }
        return false;
    }
    
    @Override
    public void next() throws RecordException, AdapterException {
        while (!this.evaluateChar(this.stateMachine.getChar())) {}
    }
    
    @Override
    public boolean hasNext() {
        return false;
    }
    
    @Override
    public void reset() {
        this.hashValue = 0;
    }
    
    @Override
    public void close() {
        synchronized (CharContinousHashParser.lock) {
            if (this.reference != null) {
                --this.reference.instanceCount;
                if (this.reference.instanceCount == 0) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("No instance refereing to this lookup table, clearing it. Key {" + CharContinousHashParser.lookupMap + "}"));
                    }
                    final LookupReference ref = CharContinousHashParser.lookupMap.get(this.lookupKey);
                    ref.clear();
                    CharContinousHashParser.lookupMap.remove(this.lookupKey);
                }
            }
        }
        this.localHashLookup = null;
        this.localEventLookupTable = null;
        this.listOfExpressions.clear();
        this.localEvent = null;
    }
    
    static {
        CharContinousHashParser.lock = new Object();
    }
    
    static class LookupReference
    {
        char[] ordinalHash;
        short[] groupHash;
        int[] groupToHash;
        SMEvent[] eventLookup;
        short[] hashLookup;
        short BIT_SHIFT;
        int instanceCount;
        
        public LookupReference() {
            this.BIT_SHIFT = 8;
            this.ordinalHash = new char[65536];
            this.hashLookup = new short[65536];
            this.groupHash = new short[2097152];
            this.groupToHash = new int[65536];
            this.eventLookup = new SMEvent[2097152];
        }
        
        public void clear() {
            this.ordinalHash = null;
            this.hashLookup = null;
            this.groupHash = null;
            this.groupToHash = null;
            this.eventLookup = null;
        }
    }
}
