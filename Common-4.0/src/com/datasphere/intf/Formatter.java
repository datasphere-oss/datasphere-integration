package com.datasphere.intf;

import com.datasphere.recovery.*;
import com.datasphere.uuid.*;
/*
 * 格式化器根接口
 */
public interface Formatter
{
    byte[] format(final Object p0) throws Exception;
    
    byte[] addHeader() throws Exception;
    
    byte[] addFooter() throws Exception;
    
    Position convertBytesToPosition(final byte[] p0, final UUID p1) throws Exception;
}
