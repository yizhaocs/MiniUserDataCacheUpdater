package com.yizhao.miniudcu.util.DatabaseUtil;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseUpdateUtil {
    private static final Logger log = Logger.getLogger(DatabaseUpdateUtil.class);

    /**
     * Execute a sql update statement ( that requires no bind params )
     *
     * autocommit
     *
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
    public static int executeUpdate(DataSource ds, String sql ) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        log.debug( String.format( "executeUpdate: %s", sql ) );

        try {
            conn = ds.getConnection();
            conn.setAutoCommit( true );
            stmt = conn.createStatement();
            return stmt.executeUpdate( sql );
        } catch( Exception e ) {
            log.error( String.format( "Error executing: %s", sql ), e );
            throw e;
        } finally {
            DatabaseCloseUtil.close( rs, stmt, conn );
        }

    }

    /**
     * Execute a sql update statement ( with bind params )
     *
     * @param ds
     * @param sql
     * @param bindValues
     * @return
     * @throws Exception
     */
    public static int executeUpdate( DataSource ds, String sql, Object[] bindValues ) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        log.debug( String.format( "executeUpdate: %s, %s", sql, Arrays.asList(bindValues) ) );

        try {
            conn = ds.getConnection();
            conn.setAutoCommit( false );
            stmt = conn.prepareStatement( sql );
            int idx = 1;
            for( Object bindValue : bindValues ){
                stmt.setObject( idx++, bindValue );
            }
            int retval = stmt.executeUpdate();
            conn.commit();
            return retval;
        } catch( Exception e ) {
            DatabaseRollbackUtil.rollbackConnection( conn );
            log.error( String.format( "Error executing: %s, %s", sql, Arrays.asList(bindValues) ), e );
            throw e;
        } finally {
            DatabaseCloseUtil.close( rs, stmt, conn );
        }

    }

    /**
     * Execute a sql update statement ( with bind params )
     * do a batch of rows
     *
     * @param ds
     * @param sql
     * @param rowBindValues
     * @return
     * @throws Exception
     */
    public static int[] executeUpdate( DataSource ds, String sql, List<Object[]> rowBindValues ) throws Exception {

        if( log.isDebugEnabled() ){
            log.debug( String.format( "executeUpdate: %s [%s rows]", sql, rowBindValues.size() ) );
        }

        if( rowBindValues.size() > 0 ){
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                conn = ds.getConnection();
                conn.setAutoCommit( false );
                stmt = conn.prepareStatement( sql );
                for( Object[] bindValues : rowBindValues ){
                    int idx = 1;
                    for( Object bindValue : bindValues ){
                        stmt.setObject( idx++, bindValue );
                    }
                    stmt.addBatch();
                }
                int[] retval = stmt.executeBatch();

                conn.commit();

                return retval;
            } catch( Exception e ) {
                DatabaseRollbackUtil.rollbackConnection( conn );
                log.error( String.format( "Error executing: %s [%s rows]", sql, rowBindValues ), e );
                throw e;
            } finally {
                DatabaseCloseUtil.close( rs, stmt, conn );
            }
        }else{
            //no rows to insert
            return new int[0];
        }

    }
}
