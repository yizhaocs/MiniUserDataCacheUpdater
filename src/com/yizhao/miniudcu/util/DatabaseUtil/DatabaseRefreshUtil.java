package com.yizhao.miniudcu.util.DatabaseUtil;

import com.yizhao.miniudcu.util.GenericObjectUtils.Triplet;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yzhao on 4/17/17.
 */
public class DatabaseRefreshUtil {
    private static final Logger log = Logger.getLogger(DatabaseRefreshUtil.class);
    public static Triplet<Set<Integer>, Set<Integer>, Set<Integer>> populateDataProviderKeys(DataSource dataSource) throws SQLException {
        Set<Integer> ekvKeys = new HashSet<Integer>();
        Set<Integer> ckvKeys = new HashSet<Integer>();
        Set<Integer> bidgenKeys = new HashSet<Integer>();

        if (dataSource != null) {
            String query = "SELECT key_id, is_fast_track, usage_type FROM marketplace.data_provider_keys";

            Connection connection = null;
            Statement s = null;
            ResultSet rs = null;
            try {
                connection = dataSource.getConnection();
                s = connection.createStatement();
                s.executeQuery(query);
                rs = s.getResultSet();

                while (rs.next()) {
                    Integer keyId = rs.getInt("key_id");
                    Integer isFastTrack = rs.getInt("is_fast_track");
                    String eventType = rs.getString("usage_type");

                    if ("E".equals(eventType))
                        ekvKeys.add(keyId);

                    // is_fast_track :
                    //	0 - ckv only
                    //	1 - bidgen and ckv
                    //	2 - bidgen only
                    //	3 - neither bidgen or ckv
                    if (isFastTrack==null || isFastTrack==0 || isFastTrack==1) {
                        ckvKeys.add(keyId);
                    }

                    if (isFastTrack==null || isFastTrack==1 || isFastTrack==2) {
                        bidgenKeys.add(keyId);
                    }
                }

            } finally {
                DatabaseCloseUtil.close(rs, s, connection);
            }
        }
        else {
            log.error("market place data source is not defined, won't pull event keys");
        }

        return new Triplet<Set<Integer>, Set<Integer>, Set<Integer>>(ekvKeys, ckvKeys, bidgenKeys);
    }
}
