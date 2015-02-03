package com.edwin.sharding.spring.mybatis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import lombok.Getter;
import lombok.Setter;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.NestedIOException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.edwin.sharding.strategy.Strategy;
import com.google.common.collect.Maps;

/**
 * @author jinming.wu
 * @date 2015-1-6
 */
public class ShardSqlSessionFactoryBean implements InitializingBean, ApplicationContextAware {

    private ApplicationContext             applicationContext;

    @Getter
    private DataSource                     mainDataSource;

    @Getter
    private SqlSessionFactory              mainSqlSessionFactory;

    @Setter
    private List<DataSource>               shardDataSourceList;

    @Getter
    private Map<String, DataSource>        shardDataSourceMap;

    @Getter
    private Map<String, SqlSessionFactory> shardSqlSessionFactory;

    @Setter
    private TransactionFactory             transactionFactory;

    @Setter
    private Resource[]                     mapperLocations;

    @Setter
    private Map<String, Class<?>>          strategyConfig;

    @Getter
    private Map<String, Strategy>          strategyMap = Maps.newLinkedHashMap();

    @Override
    public void afterPropertiesSet() throws Exception {

        if (mainDataSource == null || CollectionUtils.isEmpty(shardDataSourceList)) {
            throw new IllegalArgumentException(
                                               "Property 'mainDataSource' and property 'shardDataSourceList' can not be null together! ");
        }

        if (!CollectionUtils.isEmpty(shardDataSourceList)) {

            shardDataSourceMap = Maps.newLinkedHashMap();

            Map<String, DataSource> dataSourceMap = applicationContext.getBeansOfType(DataSource.class);
            for (Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
                if (shardDataSourceList.contains(entry.getValue())) {
                    DataSource dataSource = entry.getValue();
                    if (dataSource instanceof TransactionAwareDataSourceProxy) {
                        dataSource = ((TransactionAwareDataSourceProxy) dataSource).getTargetDataSource();
                    }
                    shardDataSourceMap.put(entry.getKey(), dataSource);
                }
            }
        }

        if (mainDataSource == null) {
            if (shardDataSourceList.get(0) instanceof TransactionAwareDataSourceProxy) {
                this.mainDataSource = ((TransactionAwareDataSourceProxy) shardDataSourceList.get(0)).getTargetDataSource();
            } else {
                mainDataSource = shardDataSourceMap.get(0);
            }
        }

        // 获取mainSqlSessionFactory
        this.mainSqlSessionFactory = buildSqlSessionFactory(mainDataSource);

        if (!CollectionUtils.isEmpty(shardDataSourceMap)) {
            shardSqlSessionFactory = Maps.newLinkedHashMap();
            for (Entry<String, DataSource> entry : shardDataSourceMap.entrySet()) {
                shardSqlSessionFactory.put(entry.getKey(), buildSqlSessionFactory(entry.getValue()));
            }
        }

        if (!CollectionUtils.isEmpty(strategyConfig)) {
            for (Map.Entry<String, Class<?>> entry : strategyConfig.entrySet()) {
                Class<?> clazz = entry.getValue();
                if (!Strategy.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException("Class " + clazz.getName() + " is not the subclass of Strategy.");
                }
                try {
                    strategyMap.put(entry.getKey(), (Strategy) (entry.getValue().newInstance()));
                } catch (Exception e) {
                    throw new RuntimeException("Class " + clazz.getName() + " can not be instance.", e);
                }
            }
        }
    }

    // copy from sqlsessionFactoryBean in mybatis-spring
    private SqlSessionFactory buildSqlSessionFactory(DataSource dataSource) throws IOException {

        if (this.mainSqlSessionFactory != null) {
            return this.mainSqlSessionFactory;
        }

        Configuration configuration = new Configuration();

        if (transactionFactory == null) {
            transactionFactory = new SpringManagedTransactionFactory(dataSource);
        }

        Environment environment = new Environment(ShardSqlSessionFactoryBean.class.getSimpleName(), transactionFactory,
                                                  dataSource);
        configuration.setEnvironment(environment);

        if (!ObjectUtils.isEmpty(this.mapperLocations)) {
            for (Resource mapperLocation : this.mapperLocations) {
                if (mapperLocation == null) {
                    continue;
                }
                String path;
                if (mapperLocation instanceof ClassPathResource) {
                    path = ((ClassPathResource) mapperLocation).getPath();
                } else {
                    path = mapperLocation.toString();
                }

                try {
                    XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(mapperLocation.getInputStream(),
                                                                             configuration, path,
                                                                             configuration.getSqlFragments());
                    xmlMapperBuilder.parse();
                } catch (Exception e) {
                    throw new NestedIOException("Failed to parse mapping resource: '" + mapperLocation + "'", e);
                } finally {
                    ErrorContext.instance().reset();
                }
            }
        }

        return new SqlSessionFactoryBuilder().build(configuration);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setMainDataSource(DataSource mainDataSource) {
        if (mainDataSource instanceof TransactionAwareDataSourceProxy) {
            this.mainDataSource = ((TransactionAwareDataSourceProxy) mainDataSource).getTargetDataSource();
        } else {
            this.mainDataSource = mainDataSource;
        }
    }
}
