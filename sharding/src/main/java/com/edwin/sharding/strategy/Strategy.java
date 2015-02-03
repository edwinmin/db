package com.edwin.sharding.strategy;

import javax.sql.DataSource;

/**
 * @author jinming.wu
 * @date 2015-1-4
 */
public interface Strategy {
    
    /**
     * 获取datasource
     * 
     * @return
     */
    public DataSource getTargetDataSource();

    /**
     * 获取目标执行sql
     * 
     * @return
     */
    public String getTargetSql();
}
