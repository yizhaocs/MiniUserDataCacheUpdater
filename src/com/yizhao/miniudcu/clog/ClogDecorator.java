package com.yizhao.miniudcu.clog;

import java.io.File;

/**
 * Created by yzhao on 4/15/17.
 */
public interface ClogDecorator {

    /**
     * Generate the rollover file name
     * @param tmpFile the tmp file being logged to prior to rollover
     * @return  the rollover file name
     */
    public String generateRolloverFileName( File tmpFile );
}
