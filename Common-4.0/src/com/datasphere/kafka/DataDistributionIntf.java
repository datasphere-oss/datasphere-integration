package com.datasphere.kafka;

import java.util.*;
/*
 * 数据分布接口
 */
public interface DataDistributionIntf
{
    Map<Integer, Long> getDataDistribution() throws Exception;
}
