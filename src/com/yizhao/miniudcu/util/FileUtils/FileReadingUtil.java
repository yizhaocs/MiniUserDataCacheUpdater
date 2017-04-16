package com.yizhao.miniudcu.util.FileUtils;

import com.yizhao.miniudcu.util.OtherUtils.ArgumentsUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileReadingUtil {

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

    /**
     * Counts number of lines in a given file.
     *
     * @param file the file the number of lines of which need to be counted.
     * @throws IOException if an exception occurs while counting number of lines
     *           in the file.
     * @throws NullPointerException if a null object is passed as an argument.
     */
    public static long countNumberOfLines(File file) throws IOException {
        ArgumentsUtil.validateMandatory(file);
        long lineCount = 0;
        if (file.exists()) {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String line = null;

            do {
                line = in.readLine();
                if (line != null)
                    lineCount++;
            } while (line != null);
        }
        return lineCount;
    }

}
