package com.yizhao.miniudcu.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileCompressionUtil {
    /**
     * Compress a file and return the zipped file
     *
     * @param file - file to be zipped
     * @param zipFile - the output file containing compressed contents
     * @throws IOException
     */
    public static File zipFile(File file, File zipFile) throws IOException {
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(file);
            out = new GZIPOutputStream(new FileOutputStream(zipFile));
            byte[] buffer = new byte[8192];
            int len = 0;
            while ((len = in.read(buffer)) >= 0)
                out.write(buffer, 0, len);
        } finally {
            FileCloseUtil.close(in);
            FileCloseUtil.close(out);
        }
        return zipFile;
    }
}
