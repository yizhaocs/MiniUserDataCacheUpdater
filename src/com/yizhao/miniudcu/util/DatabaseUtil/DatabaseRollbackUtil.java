package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.log4j.Logger;

import java.sql.Connection;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseRollbackUtil {
    private static final Logger log = Logger.getLogger(DatabaseRollbackUtil.class);
    public static void rollbackConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (Exception e) {
                log.error( "rollbackConnection()", e );
            }
        }
    }
}
