package com.yizhao.miniudcu.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileCopyUtil {
    /**
     * Copy one file to another overwriting, if the destination file exists.
     *
     * @param fromFile - the source file.
     * @param toFile - the destination file.
     * @throws IOException, if any I/O related exception occurs.
     */
    public static void copyFile(File fromFile, File toFile) throws IOException {
        InputStream in = new FileInputStream(fromFile);
        OutputStream out = new FileOutputStream(toFile);
        byte[] buf = new byte[1024 * 8];
        int len;
        try {
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
        } finally {
            FileCloseUtil.close(in);
            FileCloseUtil.close(out);
        }
    }

    /**
     * Copy all files (no directories) from one directory to another
     *
     * @param fromDir - the source directory.
     * @param toDir - the destination directory.
     * @throws IOException, if any I/O related exception occurs.
     */
    public static void copyFilesOnly(File fromDir, File toDir) throws IOException {
        FileVerifyUtil.verifyDirectoriesAreReadable(new File[] { fromDir, toDir });
        FileVerifyUtil.verifyDirectoriesAreWriteable(new File[] { toDir });
        File[] allFiles = fromDir.listFiles();
        for (File f : allFiles) {
            if (f.isFile()) {
                File toFile = new File(toDir, f.getName());
                copyFile(f, toFile);
            }
        }
    }

    public static void copyFiles(File fromDir, File toDir) throws IOException {
        FileVerifyUtil.verifyDirectoriesAreReadable(new File[]{fromDir, toDir});
        FileVerifyUtil.verifyDirectoriesAreWriteable(new File[] {toDir});
        File[] allFiles = fromDir.listFiles();
        for(File f: allFiles) {
            File toFile = new File(toDir, f.getName());
            copyFile(f, toFile);
        }
    }


}
