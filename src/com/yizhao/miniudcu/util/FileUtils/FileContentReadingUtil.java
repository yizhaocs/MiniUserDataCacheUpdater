package com.yizhao.miniudcu.util.FileUtils;

import java.io.File;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileContentReadingUtil {

    /**
     * Get file prefix and suffix in an array - first element is prefix, second is
     * suffix.
     *
     * @param file - file
     * @return file prefix and suffix in an array - first element is prefix,
     *         second is suffix.
     */
    public static String[] getFilePrefixSuffix(File file) {
        String origFileName = file.getName();
        String[] origFilePrefixSuffix = origFileName.split("\\.");
        String origFilePrefix = origFilePrefixSuffix[0];
        String origFileSuffix = origFilePrefixSuffix.length == 2 ? "." + origFilePrefixSuffix[1] : "";
        return new String[] { origFilePrefix, origFileSuffix };
    }

}
