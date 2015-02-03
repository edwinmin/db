package com.edwin.sharding.spring.mybatis;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.edwin.sharding.Shard;
import com.edwin.sharding.strategy.Strategy;
import com.google.common.collect.Maps;

/**
 * @author jinming.wu
 * @date 2015-1-7
 */
public abstract class ShardSqlSessionDaoSupport implements InitializingBean {

    private SqlSession                          sqlSession;

    private Map<DataSource, SqlSessionTemplate> dataSourceMap;

    private ShardSqlSessionFactoryBean          shardSqlSessionFactory;

    public final void setSqlSessionFactory(ShardSqlSessionFactoryBean shardSqlSessionFactory) {

        this.shardSqlSessionFactory = shardSqlSessionFactory;

        this.sqlSession = (SqlSession) Proxy.newProxyInstance(ShardSqlSessionDaoSupport.class.getClassLoader(),
                                                              new Class[] { SqlSession.class }, new SqlSessionHandler());
    }

    @Override
    public final void afterPropertiesSet() throws Exception {

        dataSourceMap = Maps.newLinkedHashMap();

        dataSourceMap.put(shardSqlSessionFactory.getMainDataSource(),
                          new SqlSessionTemplate(shardSqlSessionFactory.getMainSqlSessionFactory()));

        Map<String, DataSource> shardDataSources = shardSqlSessionFactory.getShardDataSourceMap();

        if (!CollectionUtils.isEmpty(shardDataSources)) {

            for (Entry<String, DataSource> entry : shardDataSources.entrySet()) {

                SqlSessionFactory sqlSessionFactory = shardSqlSessionFactory.getShardSqlSessionFactory().get(entry.getKey());

                dataSourceMap.put(entry.getValue(), new SqlSessionTemplate(sqlSessionFactory));
            }
        }
    }

    private class SqlSessionHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            DataSource targetDS = shardSqlSessionFactory.getMainDataSource();

            if (ObjectUtils.isEmpty(args)) {

            }

            // 如果是分库分表场景
            if (args.length > 1 && args[1] instanceof Shard) {

                Shard shard = (Shard) args[1];
                String statement = (String) args[0];
                Strategy strategy = shardSqlSessionFactory.getStrategyMap().get(shard.getShardName());
                SqlSessionTemplate sqlSessionTemplate = null;

                // 获取最终的SQL
                Configuration configuration = shardSqlSessionFactory.getMainSqlSessionFactory().getConfiguration();
                MappedStatement mappedStatement = configuration.getMappedStatement(statement);
                BoundSql boundSql = mappedStatement.getBoundSql(wrapCollection(shard.getParams()));

                boundSql.getSql();
                
                targetDS = strategy.getTargetDataSource();
                if (targetDS == null || (sqlSessionTemplate = dataSourceMap.get(targetDS)) == null) {
                    targetDS = shardSqlSessionFactory.getMainDataSource();
                    sqlSessionTemplate = dataSourceMap.get(targetDS);
                }

                // 执行代理后的方法
                return method.invoke(sqlSessionTemplate, args);
            }

            return null;
        }
    }

    // 不允许重载
    public final SqlSession getSqlSession() {
        return this.sqlSession;
    }

    private Object wrapCollection(Object object) {

        Map<String, Object> resultMap = Maps.newHashMap();

        if (object instanceof List) {
            resultMap.put("list", object);
            return resultMap;
        } else if (object != null && object.getClass().isArray()) {
            resultMap.put("array", object);
            return resultMap;
        }

        return object;
    }
}
