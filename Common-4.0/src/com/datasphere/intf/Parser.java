package com.datasphere.intf;

import java.io.*;
import java.util.*;

import com.datasphere.event.*;
/*
 * 解析器器根接口
 */
public interface Parser
{
    Iterator<Event> parse(final InputStream p0) throws Exception;
    
    void close() throws Exception;
}
