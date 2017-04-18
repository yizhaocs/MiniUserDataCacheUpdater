package com.yizhao.miniudcu.util.FileUtils;

import com.yizhao.miniudcu.util.ArgumentUtils.ArgumentsUtil;

import java.io.File;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileCreateUtil {
    /**
     * Create directory with all parent directories
     *
     * @param directory the directory to create
     */
    public static void createDirectory(File directory) throws IOException {
        ArgumentsUtil.validateMandatory(directory);
        if (!directory.exists()) {
            if (!directory.mkdirs())
                throw new IOException("Failed to create directory " + directory);
        }
    }

    /**
     * Create directories. Ignores, if the directory already exists.
     *
     * @param directories
     * @throws IOException
     */
    public static void createDirectories(File[] directories) throws IOException {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (File file : directories) {
            if (file != null) {
                createDirectory(file);
            }
        }
    }
}
