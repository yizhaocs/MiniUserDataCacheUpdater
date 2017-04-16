package com.yizhao.miniudcu.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileRenameUtil {
    /**
     * Rename file.
     *
     * @param fromFile the file to move
     * @param toFile - the destination file.
     * @param overwrite whether or not to overwrite existing file in the
     *          destination file's directory.
     */
    public static void renameFile(File fromFile, File toFile, boolean overwrite) throws IOException {
        if (!fromFile.exists())
            return;
        FileMoveUtil.doMoveFile(fromFile, toFile, overwrite);
    }
}
