package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseReadingUtil {
    private static final Logger log = Logger.getLogger(DatabaseReadingUtil.class);

    /**
     * Count the number of rows in the table
     * @param ds
     * @param tableName
     * @return # of rows in the table
     * @throws Exception
     */
    public static long countRows(DataSource ds, String tableName ) throws Exception {
        return countRows( ds, tableName, null );
    }

    /**
     * Count the number of rows in the table, subject to the where clause
     * @param ds
     * @param tableName
     * @param where e.g. a = 1 and b > 3
     * @return # of rows in the table
     * @throws Exception
     */
    public static long countRows( DataSource ds, String tableName, String where ) throws Exception {
        long retval = 0;

        String sql = "SELECT COUNT(*) FROM " + tableName;
        if( where != null && where.length() > 0 ){
            sql += " WHERE " + where;
        }

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try{

            conn = ds.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery( sql );
            if( rs.next() ){
                retval = rs.getLong( 1 );
            }

        }catch( Exception e ){
            log.error( "countRows()", e );
            throw e;
        }finally{
            DatabaseCloseUtil.closeResultSet( rs );
            DatabaseCloseUtil.closeStatement( stmt );
            DatabaseCloseUtil.closeConnection( conn );
        }

        return retval;
    }
}
