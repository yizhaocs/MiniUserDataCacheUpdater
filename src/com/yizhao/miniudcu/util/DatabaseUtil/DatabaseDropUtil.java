package com.yizhao.miniudcu.util.DatabaseUtil;

import javax.sql.DataSource;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseDropUtil {
    /**
     * Drop table ( if exists )
     * @throws Exception
     */
    public static int dropTable(DataSource ds, String tableName ) throws Exception {
        String sql = String.format( " DROP TABLE IF EXISTS %s ", tableName );
        return DatabaseUpdateUtil.executeUpdate( ds, sql );
    }

}
