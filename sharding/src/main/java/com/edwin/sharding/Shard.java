package com.edwin.sharding;

import java.io.Reader;

import lombok.Data;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import com.edwin.common.tools.lang.StringHelper;

/**
 * @author jinming.wu
 * @date 2015-1-4
 */
@Data
public class Shard {

    /** 切分依赖的key */
    private Object   shardKey;

    /** 切分策略名称 */
    private String   shardName;

    /** 执行sql的参数 */
    private Object[] params;

    public Shard() {
    }

    public Shard(String shardName, Object shardKey, Object... params) {
        this.shardKey = shardKey;
        this.shardName = shardName;
        this.params = params;
    }

    @Override
    public String toString() {
        return StringHelper.join("ShardParam: 【shardName=" + shardName + ", shardKey=" + shardKey + ", params="
                                 + ", params=" + "】");
    }

    public static void main(String args[]) throws Exception {

        String resource = "mybatis.cfg.xml";

        Reader reader = Resources.getResourceAsReader(resource);

        SqlSessionFactory ssf = new SqlSessionFactoryBuilder().build(reader);

        SqlSession session = ssf.openSession();
    }
}
