package com.yizhao.miniudcu.clog;

import com.yizhao.miniudcu.util.FileUtils.FileReadingUtil;

import java.io.File;
import java.util.Random;

/**
 * Created by yzhao on 4/15/17.
 */

public class ETLClogDecorator implements ClogDecorator {

    /**
     * For ETL files, the rollover name is just the tmp file name + random number + timestamp
     * e.g. bid_info_netezza_ny.csv -> bid_info_netezza_ny-1003152923-1344401532787.csv
     */
    public String generateRolloverFileName( File tmpFile ) {
        String[] prefixSuffix = FileReadingUtil.getFilePrefixSuffix( tmpFile );
        return String.format("%s-%s-%s%s",
                prefixSuffix[0], random.nextInt( Integer.MAX_VALUE ), System.currentTimeMillis(), prefixSuffix[1]);
    }

    private final Random random = new Random();
}
