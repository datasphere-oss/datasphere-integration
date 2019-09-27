package com.datasphere.source.smlite;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.reader.*;
import com.datasphere.common.exc.*;

public class StateMachineBuilder
{
    private static final String LITE = "Lite";
    private static final String EXTN = "Extn";
    private static final String PATTERN = "Patt";
    SMProperty smProperty;
    
    public StateMachineBuilder(final SMProperty prop) {
        this.smProperty = prop;
    }
    
    private boolean hasMultiCharacterDelimiter() {
        return this.checkForMultiCharacter(this.smProperty.timeStamp) || this.checkForMultiCharacter(this.smProperty.escapeList) || this.checkForMultiCharacter(this.smProperty.columnDelimiterList) || this.checkForMultiCharacter(this.smProperty.rowDelimiterList) || this.checkForMultiCharacter(this.smProperty.recordBegin);
    }
    
    private boolean checkForMultiCharacter(final String[] delimiter) {
        for (int itr = 0; delimiter != null && itr < delimiter.length; ++itr) {
            if (delimiter[itr].length() > 1) {
                return true;
            }
        }
        return false;
    }
    
    private boolean hasRegExDelimiter() {
        return false;
    }
    
    private StateMachine createStateMachine(final String smName, final Reader dataSource, final Property prop) {
        StateMachine sm = null;
        if (smName.equals("Lite")) {
            sm = new StateMachine(dataSource, new SMProperty(prop.getMap()));
        }
        else if (smName.equals("Extn")) {
            sm = new StateMachineExtn(dataSource, new SMProperty(prop.getMap()));
        }
        return sm;
    }
    
    private CharParser createParser(final StateMachine sm, final String parserName) throws AdapterException {
        CharParser parser = null;
        if (parserName.equals("Lite")) {
            parser = new CharContinousHashParser(sm, this.smProperty);
        }
        else {
            if (!parserName.equals("Extn")) {
                throw new AdapterException("Unsupported parser [" + parserName + "]");
            }
            try {
                parser = new CharContinousHashParser(sm, this.smProperty);
            }
            catch (Exception e) {
                parser = new CharParserExtn(sm, this.smProperty);
            }
        }
        return parser;
    }
    
    public StateMachine createStateMachine(final Reader reader, final Property prop) throws AdapterException {
        String charParser;
        String stateMachine;
        if (this.hasRegExDelimiter()) {
            charParser = "Patt";
            stateMachine = "Extn";
        }
        else if (!this.hasMultiCharacterDelimiter()) {
            charParser = "Lite";
            stateMachine = "Lite";
        }
        else {
            charParser = "Extn";
            stateMachine = "Extn";
        }
        final StateMachine sm = this.createStateMachine(stateMachine, reader, prop);
        if (sm != null) {
            sm.init();
            final CharParser parser = this.createParser(sm, charParser);
            if (parser != null) {
                parser.init();
                sm.parser(parser);
                return sm;
            }
        }
        throw new AdapterException("Couldn't create appropriate state machine for the given property");
    }
}
