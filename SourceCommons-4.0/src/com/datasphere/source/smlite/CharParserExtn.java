package com.datasphere.source.smlite;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.common.exc.*;

public class CharParserExtn extends CharParser
{
    Logger logger;
    protected final short START_OF_DELIMITER = 16;
    protected final short END_OF_DELIMITER = 32;
    protected final short PART_OF_DELIMITER = 64;
    protected short[] hash;
    long hashValue;
    short eventType;
    Map<Integer, String[]> eventToDelimiter;
    short[] specialCharHash;
    short[] specialChars;
    Map<Long, SMEvent> eventMap;
    Map<String, SMEvent> delimiterToEventMap;
    Map<String, SMEvent> defaultDelimiterToEventMap;
    List<String> possibleCombinations;
    List<String> allDelimiters;
    Map<Integer, String[]> eventToDelimiterMap;
    Map<String, short[]> eventToHashMap;
    Map<Integer, String[]> specialDelimiters;
    String otherChars;
    protected short lastCharValue;
    protected short[] charToCodeMap;
    protected char[] codeToCharMap;
    short[] defaultEventType;
    long bitMask;
    int bitCnt;
    int charMask;
    int maxPossibleDelimiterLength;
    int maxPossibleCombinationLength;
    int groupStartId;
    
    public CharParserExtn(final StateMachine sm, final SMProperty prop) {
        super(sm, prop);
        this.logger = Logger.getLogger((Class)CharParserExtn.class);
        this.lastCharValue = 0;
        this.defaultEventType = new short[] { 2, 1, 3, 4, 5 };
    }
    
    @Override
    protected void init() {
        super.init();
        this.initializeInternalDelimiters();
        this.initializeEventToDelimiterMap();
        this.initLookupTable();
    }
    
    private int maxPossibleDelimiterLength() {
        int len = 0;
        for (int itr = 0; itr < this.colDelimiter.length; ++itr) {
            if (this.colDelimiter[itr].length() > len) {
                len = this.colDelimiter[itr].length();
            }
        }
        for (int itr = 0; itr < this.rowDelimiter.length; ++itr) {
            if (this.rowDelimiter[itr].length() > len) {
                len = this.rowDelimiter[itr].length();
            }
        }
        return len * 2 - 1;
    }
    
    @Override
    public void validateProperty() throws AdapterException {
        String delimiters = "";
        for (int itr = 0; itr < this.colDelimiter.length; ++itr) {
            if (delimiters.indexOf(this.colDelimiter[itr]) != -1) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Column delimiter [" + this.colDelimiter[itr] + "] is over-lapped with other delimiter, you may not get this event"));
                }
            }
            else {
                delimiters = delimiters + "," + this.colDelimiter[itr];
            }
        }
        for (int itr = 0; itr < this.rowDelimiter.length; ++itr) {
            if (delimiters.indexOf(this.colDelimiter[itr]) != -1) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Row delimiter [" + this.rowDelimiter[itr] + "] is over-lapped with other delimiter, you may not get this event"));
                }
            }
            else {
                delimiters = delimiters + "," + this.rowDelimiter[itr];
            }
        }
        this.hashValue = 1L;
        this.hashValue <<= this.maxPossibleDelimiterLength * this.bitCnt;
        if (this.hashValue < 0L) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Bit Count : [" + this.bitCnt + "]"));
                this.logger.debug((Object)("Max combination length [" + this.maxPossibleDelimiterLength + "]"));
                this.logger.debug((Object)"Parser couldn't handle the given combination of delimiter");
            }
            throw new AdapterException("Parser couldn't handle the given combination of delimiter");
        }
    }
    
    private void initializeEventToDelimiterMap() {
        if (this.eventToDelimiterMap == null) {
            this.eventToDelimiterMap = new HashMap<Integer, String[]>();
        }
        this.eventToDelimiterMap.put(1, this.prop.columnDelimiterList);
        this.eventToDelimiterMap.put(2, this.prop.rowDelimiterList);
        this.eventToDelimiterMap.put(3, this.prop.quoteSetList);
    }
    
    private short[] createHash(short[] eventType) {
        final StringBuilder eventStringBuilder = new StringBuilder();
        short[] hash = null;
        if (eventType == null) {
            eventType = this.defaultEventType;
        }
        for (int eventItr = 0; eventItr < eventType.length; ++eventItr) {
            eventStringBuilder.append("," + Integer.toString(eventType[eventItr]));
        }
        final String eventString = eventStringBuilder.toString();
        if (this.eventToHashMap == null) {
            this.eventToHashMap = new HashMap<String, short[]>();
        }
        if ((hash = this.eventToHashMap.get(eventString)) == null) {
            final short[] newHash = new short[65536];
            boolean proceed = true;
            for (final Map.Entry<Integer, String[]> entry : this.eventToDelimiterMap.entrySet()) {
                final int event = entry.getKey();
                proceed = false;
                for (int eventItr2 = 0; eventItr2 < eventType.length; ++eventItr2) {
                    if (eventType[eventItr2] != event) {
                        proceed = true;
                        break;
                    }
                }
                if (proceed) {
                    String[] delimiterList = this.eventToDelimiterMap.get(event);
                    final String[] tmp = this.specialDelimiters.get(event);
                    if (tmp != null) {
                        final List<String> list = new ArrayList<String>();
                        if (delimiterList != null) {
                            for (int dItr = 0; dItr < delimiterList.length; ++dItr) {
                                list.add(delimiterList[dItr]);
                            }
                        }
                        for (int dItr = 0; dItr < tmp.length; ++dItr) {
                            list.add(tmp[dItr]);
                        }
                        delimiterList = list.toArray(new String[list.size()]);
                    }
                    if (delimiterList == null) {
                        continue;
                    }
                    for (int strItr = 0; strItr < delimiterList.length; ++strItr) {
                        final int len = delimiterList[strItr].length();
                        final String delimiter = delimiterList[strItr];
                        short flag = 0;
                        for (int charItr = 0; charItr < len; ++charItr) {
                            if (len != charItr + 1) {
                                flag = 64;
                            }
                            else {
                                flag = 32;
                            }
                            newHash[delimiter.charAt(charItr)] = flag;
                        }
                    }
                }
            }
            this.eventToHashMap.put(eventString, newHash);
            hash = newHash;
        }
        return hash;
    }
    
    protected void initializeInternalDelimiters() {
        if (this.specialDelimiters == null) {
            this.specialDelimiters = new HashMap<Integer, String[]>();
            final List<String> list = new ArrayList<String>();
            this.otherChars = "";
            for (short itr = 0; itr < 13; ++itr) {
                list.clear();
                final String additionalChars = this.additionalChars(itr);
                this.otherChars += additionalChars;
                final String checkpointChars = this.checkpointChars(itr);
                for (int charItr = 0; charItr < checkpointChars.length(); ++charItr) {
                    if (charItr == 0) {
                        list.add(additionalChars + checkpointChars.charAt(charItr));
                    }
                    else {
                        list.add("" + checkpointChars.charAt(charItr));
                    }
                }
                this.specialDelimiters.put((int)itr, list.toArray(new String[list.size()]));
            }
        }
    }
    
    protected void initializeCharToCodeMap() {
        this.initializeCharToCodeMap(this.colDelimiter, this.charToCodeMap, this.codeToCharMap);
        this.initializeCharToCodeMap(this.rowDelimiter, this.charToCodeMap, this.codeToCharMap);
        if (this.prop.quoteSetList != null) {
            this.initializeCharToCodeMap(this.prop.quoteSetList, this.charToCodeMap, this.codeToCharMap);
        }
        final int cnt = this.noOfSpecialChars();
        if (cnt > 0) {
            final List<String> list = new ArrayList<String>();
            this.specialCharHash = new short[cnt];
            this.specialChars = new short[cnt];
            for (int itr = 0; itr < cnt; ++itr) {
                this.specialChars[itr] = (short)(255 - cnt + itr);
                list.add("" + (char)(255 - cnt + itr));
            }
            if (this.specialDelimiters != null) {
                final String[] tmp = this.specialDelimiters.get(1);
                for (int itr2 = 0; itr2 < tmp.length; ++itr2) {
                    list.add(tmp[itr2]);
                }
                this.specialDelimiters.put(1, list.toArray(new String[list.size()]));
            }
        }
        if (this.specialDelimiters != null) {
            for (final Map.Entry<Integer, String[]> entry : this.specialDelimiters.entrySet()) {
                this.initializeCharToCodeMap(entry.getValue(), this.charToCodeMap, this.codeToCharMap);
            }
        }
    }
    
    public void initLookupTable() {
        try {
            this.hash = new short[65536];
            this.charToCodeMap = new short[65536];
            this.codeToCharMap = new char[65536];
            this.maxPossibleCombinationLength = this.maxPossibleDelimiterLength();
            this.initializeCharToCodeMap();
            this.bitCnt = this.getNumberOfBits(this.lastCharValue);
            this.initializeBitMask();
            this.validateProperty();
            this.initEventHash(this.colDelimiter, this.hash, (short)1);
            this.initEventHash(this.rowDelimiter, this.hash, (short)2);
            if (this.prop.quoteSetList != null) {
                this.initEventHash(this.prop.quoteSetList, this.hash, (short)3);
            }
            for (int eventItr = 0; eventItr < 13; ++eventItr) {
                final String[] tmp = this.getDelimiters((short)eventItr);
                if (tmp != null) {
                    this.initEventHash(tmp, this.hash, (short)eventItr);
                }
            }
            if (this.specialCharHash != null) {
                for (int charItr = 0; charItr < this.specialCharHash.length; ++charItr) {
                    this.charToCodeMap[this.specialCharHash[charItr]] = 0;
                }
            }
            (this.eventToDelimiter = new HashMap<Integer, String[]>()).put(1, this.prop.columnDelimiterList);
            this.eventToDelimiter.put(2, this.prop.rowDelimiterList);
            this.eventToDelimiter.put(3, this.prop.quoteSetList);
            (this.defaultDelimiterToEventMap = new HashMap<String, SMEvent>()).putAll(this.delimiterToEventMap);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void initializeCharToCodeMap(final String[] delimiter, final short[] codeMap, final char[] charMap) {
        for (int itr = 0; itr < delimiter.length; ++itr) {
            for (int charItr = 0; charItr < delimiter[itr].length(); ++charItr) {
                final int charIndex = delimiter[itr].charAt(charItr);
                if (codeMap[charIndex] == 0) {
                    codeMap[charIndex] = (short)(++this.lastCharValue);
                    charMap[this.lastCharValue] = delimiter[itr].charAt(charItr);
                }
            }
        }
    }
    
    private int getNumberOfBits(int value) {
        int bitCnt;
        for (bitCnt = 0; value > 0; value >>= 1, ++bitCnt) {}
        return bitCnt;
    }
    
    protected String[] getDelimiters(final short eventId) {
        return new String[0];
    }
    
    protected String additionalChars(final short eventId) {
        return "";
    }
    
    protected String checkpointChars(final short eventId) {
        return "";
    }
    
    protected int noOfSpecialChars() {
        return 0;
    }
    
    protected void initializeBitMask() {
        final int cnt;
        int bitCnt = cnt = this.getNumberOfBits(this.lastCharValue);
        int mask = 1;
        this.bitMask = 0L;
        while (bitCnt - 1 > 0) {
            mask <<= 1;
            mask |= 0x1;
            --bitCnt;
        }
        this.charMask = mask;
        for (int idx = 0; idx < this.maxPossibleCombinationLength; ++idx) {
            this.bitMask <<= cnt;
            this.bitMask |= mask;
        }
    }
    
    private void initEventHash(final String[] delimiter, final short[] hashTable, final short eventType) throws AdapterException {
        int hashValue = 0;
        for (int idx = 0; idx < delimiter.length; ++idx) {
            short flag = 0;
            if (delimiter[idx].isEmpty() || this.checkForRegEx(delimiter[idx])) {
                throw new AdapterException("Delimiter [" + delimiter[idx] + "] contains RegEx. and it is not supported.");
            }
            if (delimiter[idx].length() > 1) {
                for (int itr = 0; itr < delimiter[idx].length(); ++itr) {
                    if (hashTable[delimiter[idx].charAt(itr)] != 0) {
                        flag = hashTable[delimiter[idx].charAt(itr)];
                    }
                    else {
                        flag = 0;
                    }
                    if (itr == 0) {
                        flag |= 0x10;
                    }
                    else if (itr == delimiter[idx].length() - 1) {
                        flag |= 0x20;
                        flag |= eventType;
                    }
                    else if (flag == 0) {
                        flag = 64;
                    }
                    hashTable[(short)delimiter[idx].charAt(itr)] = flag;
                    hashValue <<= this.bitCnt;
                    hashValue += this.charToCodeMap[(short)delimiter[idx].charAt(itr)];
                }
            }
            else {
                flag |= 0x20;
                flag |= eventType;
                hashValue = this.charToCodeMap[(short)delimiter[idx].charAt(0)];
                hashTable[(short)delimiter[idx].charAt(0)] = flag;
            }
            if (this.eventMap == null) {
                this.eventMap = new TreeMap<Long, SMEvent>();
                this.delimiterToEventMap = new HashMap<String, SMEvent>();
            }
            final SMEvent eventData = EventFactory.createEvent(eventType);
            if (eventType == 2) {
                ((RowEvent)eventData).array(this.stateMachine.buffer());
            }
            eventData.length(delimiter[idx].length());
            final List<Integer> hashList = this.computeHash(delimiter[idx]);
            for (int itr2 = 0; itr2 < hashList.size(); ++itr2) {
                this.eventMap.put((long)hashList.get(itr2), eventData);
            }
            hashValue = 0;
        }
    }
    
    private List<Integer> computeHash(final String delimiter) {
        if (this.allDelimiters == null) {
            this.allDelimiters = new ArrayList<String>();
            for (int itr = 0; itr < this.colDelimiter.length; ++itr) {
                this.allDelimiters.add(this.colDelimiter[itr]);
            }
            for (int itr = 0; itr < this.rowDelimiter.length; ++itr) {
                this.allDelimiters.add(this.rowDelimiter[itr]);
            }
            if (this.prop.quoteSetList != null) {
                for (int itr = 0; itr < this.prop.quoteSetList.length; ++itr) {
                    this.allDelimiters.add(this.prop.quoteSetList[itr]);
                }
            }
            if (this.specialChars != null) {
                for (int itr = 0; itr < this.specialChars.length; ++itr) {
                    this.allDelimiters.add("" + (char)this.specialChars[itr]);
                }
            }
        }
        final Combination combination = new Combination(this.allDelimiters, delimiter, this.otherChars);
        this.possibleCombinations = combination.getCombinationLists();
        final List<Integer> hashList = new ArrayList<Integer>();
        int hashValue = this.computeHashFor(delimiter);
        hashList.add(hashValue);
        for (int combinationItr = 0; combinationItr < this.possibleCombinations.size(); ++combinationItr) {
            final String possibleDelimiter = this.possibleCombinations.get(combinationItr) + delimiter;
            hashValue = this.computeHashFor(possibleDelimiter);
            hashList.add(hashValue);
        }
        return hashList;
    }
    
    @Override
    public int computeHashFor(final String delimiter) {
        int hashValue = 0;
        for (int charItr = 0; charItr < delimiter.length(); ++charItr) {
            hashValue <<= this.bitCnt;
            hashValue += this.charToCodeMap[(short)delimiter.charAt(charItr)];
        }
        hashValue &= (int)this.bitMask;
        return hashValue;
    }
    
    private boolean checkForRegEx(final String delimiter) {
        return false;
    }
    
    private boolean evaluateChar(final short charIndex) {
        if (this.hash[charIndex] != 0) {
            this.eventType = this.hash[charIndex];
            if ((this.eventType & 0x20) != 0x0) {
                this.hashValue <<= this.bitCnt;
                this.hashValue += this.charToCodeMap[charIndex];
                this.hashValue &= this.bitMask;
                this.hashValue = this.compressHash(this.hashValue);
                final SMEvent eventData = this.lookForMatchingDelimiter(this.hashValue);
                if (eventData != null) {
                    this.stateMachine.publishEvent(eventData);
                    if (this.stateMachine.canBreak()) {
                        return true;
                    }
                }
            }
            else {
                this.hashValue <<= this.bitCnt;
                this.hashValue += this.charToCodeMap[charIndex];
            }
        }
        else {
            this.hashValue = 0L;
        }
        return false;
    }
    
    protected SMEvent lookForMatchingDelimiter(final long hashValue) {
        return this.eventMap.get(hashValue);
    }
    
    public long compressHash(final long hashValue2) {
        return hashValue2;
    }
    
    @Override
    public void reset() {
        this.hashValue = 0L;
    }
    
    @Override
    public void next() throws RecordException, AdapterException {
        this.hashValue = 0L;
        short charIndex;
        do {
            charIndex = (short)this.stateMachine.getChar();
        } while (!this.evaluateChar(charIndex));
    }
    
    @Override
    public boolean hasNext() {
        return false;
    }
    
    @Override
    public void ignoreEvents(final short[] eventTypes) {
        this.hash = this.createHash(eventTypes);
        assert this.hash != null;
    }
    
    @Override
    public void ignoreEvents(final short[] eventTypes, final SMEvent event) {
        this.ignoreEvents(eventTypes);
    }
    
    public void initializeDelimiter(final String[] delimters, final short eventType) {
    }
    
    public void initializeDelimiters() {
    }
    
    @Override
    public void finalizeDelimiterInit() {
    }
    
    private static class Combination
    {
        private List<String> combinations;
        private int maxPossibleCombinationLength;
        private int delimiterLength;
        private String otherChars;
        
        public Combination(final List<String> delimiters, final String delimiter, final String otherChars) {
            this.otherChars = otherChars;
            this.combinations = new ArrayList<String>();
            this.delimiterLength = delimiter.length();
            this.listCombinations(delimiters);
        }
        
        public void listCombinations(final List<String> list) {
            final StringBuilder combinedStringBuilder = new StringBuilder();
            int maxLen = 0;
            for (int itr = 0; itr < list.size(); ++itr) {
                combinedStringBuilder.append(list.get(itr));
                if (list.get(itr).length() > maxLen) {
                    maxLen = list.get(itr).length();
                }
            }
            this.maxPossibleCombinationLength = maxLen * 2 - 1;
            combinedStringBuilder.append(this.otherChars);
            final String combinedString = combinedStringBuilder.toString();
            for (int itr2 = 1; itr2 <= this.maxPossibleCombinationLength - this.delimiterLength; ++itr2) {
                this.createCom(combinedString, "", itr2);
            }
        }
        
        public void createCom(final String orig, final String part, int iteration) {
            if (iteration - 1 > 0) {
                --iteration;
                for (int i = 0; i < orig.length(); ++i) {
                    this.createCom(orig, part + orig.charAt(i), iteration);
                }
            }
            else {
                for (int i = 0; i < orig.length(); ++i) {
                    final String tmp = part + orig.charAt(i);
                    this.combinations.add(tmp);
                }
            }
        }
        
        public List<String> getCombinationLists() {
            return this.combinations;
        }
        
        public int maxPossibleCombinationLength() {
            return this.maxPossibleCombinationLength;
        }
    }
}
