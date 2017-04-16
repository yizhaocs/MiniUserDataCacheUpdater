package com.yizhao.miniudcu.util.FileUtils;

import java.io.File;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileDeleteUtil {



    /**
     * Delete given array of files
     * @param filesToDelete
     * @return
     */
    public static int deleteFiles(File[] filesToDelete) {
        int deletedCount = 0;
        for (int i = 0; i < filesToDelete.length; i++) {
            deletedCount += deleteFile(filesToDelete[i]);
        }
        return deletedCount;
    }

    /**
     * @param file
     * @return
     */
    public static int deleteFile(File file) {
        int deletedCount = 0;
        if (file != null && file.delete())
            deletedCount++;
        return deletedCount;
    }
}
