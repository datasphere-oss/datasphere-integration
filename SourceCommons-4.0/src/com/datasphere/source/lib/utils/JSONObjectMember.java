package com.datasphere.source.lib.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;

public class JSONObjectMember
{
    private List<Member> memberList;
    private Logger logger;
    private FieldModifier[] fieldModifiers;
    private boolean checkIfMemberAJson;
    
    public JSONObjectMember(final String memberList, final Field[] fields, final FieldModifier[] fieldModifiers) {
        this.logger = Logger.getLogger((Class)JSONObjectMember.class);
        this.checkIfMemberAJson = false;
        this.memberList = new ArrayList<Member>();
        if (fieldModifiers != null) {
            this.fieldModifiers = Arrays.copyOf(fieldModifiers, fieldModifiers.length);
        }
        if (memberList != null) {
            final String[] members = memberList.split(",");
            if (members.length == 1) {
                this.checkIfMemberAJson = true;
            }
            this.initializeMemberList(members, fields);
        }
        else {
            if (fields.length == 1) {
                this.checkIfMemberAJson = true;
            }
            this.initializeDefaultMemberList(fields);
        }
        if (this.memberList.isEmpty()) {
            this.logger.error((Object)("Initialized member list from " + memberList + " is empty. Please check if field names " + Arrays.toString(fields) + " of type of the stream match with member names"));
        }
        else if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("Initialized members list is " + this.memberList.toString()));
        }
    }
    
    public List<Member> getMemberList() {
        return Collections.unmodifiableList((List<? extends Member>)this.memberList);
    }
    
    private void initializeMemberList(final String[] members, final Field[] fields) {
        for (final String member : members) {
            int fieldIndex = 0;
            for (final Field field : fields) {
                if (field.getName().equalsIgnoreCase(member)) {
                    final Member memberInstance = new Member(member, field, fieldIndex, this.wouldFieldReturnValidJSON(field));
                    this.memberList.add(memberInstance);
                    break;
                }
                ++fieldIndex;
            }
        }
    }
    
    private void initializeDefaultMemberList(final Field[] fields) {
        int fieldIndex = 0;
        for (final Field field : fields) {
            final Member memberInstance = new Member(field.getName(), field, fieldIndex, this.wouldFieldReturnValidJSON(field));
            this.memberList.add(memberInstance);
            ++fieldIndex;
        }
    }
    
    private boolean wouldFieldReturnValidJSON(final Field field) {
        return this.checkIfMemberAJson && (Map.class.isAssignableFrom(field.getType()) || field.getType().isArray() || field.getType().getName().equals("org.apache.avro.generic.GenericRecord") || field.getType().getName().equals("com.fasterxml.jackson.databind.JsonNode"));
    }
    
    public class Member
    {
        private String name;
        private Field field;
        private int fieldIndex;
        private boolean stripBraces;
        
        public Member(final String name, final Field field, final int fieldIndex, final boolean dontAddEnclosingFieldName) {
            this.name = "";
            this.stripBraces = false;
            if (!dontAddEnclosingFieldName) {
                this.name = "\"" + name + "\":";
            }
            else {
                this.stripBraces = true;
            }
            this.field = field;
            this.fieldIndex = fieldIndex;
        }
        
        public String processValue(final Object event) throws Exception {
            String value;
            try {
                final Object object = this.field.get(event);
                if (object == null) {
                    return "null";
                }
                value = JSONObjectMember.this.fieldModifiers[this.fieldIndex].modifyFieldValue(object, event);
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                final AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, (Throwable)e);
                throw se;
            }
            if (this.stripBraces) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }
        
        public String getName() {
            return this.name;
        }
        
        @Override
        public String toString() {
            return "Member name is " + this.getName() + ". Corresponding field name is " + this.field.getName();
        }
    }
}
