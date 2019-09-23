package com.datasphere.runtime.compiler.select;

public enum PredicateType
{
    RELATION_FILTER, 
    CONSTANT_EXPRESSION, 
    TWO_RELATION_EQUIJOIN, 
    FILTER_AFTER_JOIN;
}
