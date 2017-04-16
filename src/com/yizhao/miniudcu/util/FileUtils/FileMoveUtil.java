package com.yizhao.miniudcu.util.FileUtils;

import com.yizhao.miniudcu.util.OtherUtils.ArgumentsUtil;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileMoveUtil {
    private static final Logger logger = Logger.getLogger(FileMoveUtil.class);
    /**
     * Move file to a directory, overwriting it if necessary.
     *
     * @param file
     * @param toDirectory
     * @throws IOException
     */
    public static void moveFile(File file, File toDirectory) throws IOException {
        moveFile(file, toDirectory, true);
    }

    /**
     * Move file into the given directory.
     *
     * @param file
     * @param toDirectory
     * @param overwrite
     * @throws IOException
     */
    public static void moveFile(File file, File toDirectory, boolean overwrite) throws IOException {
        if (!file.exists())
            return;
        File toFile = new File(toDirectory, file.getName());
        doMoveFile(file, toFile, overwrite);
    }

    public static void doMoveFile(File fromFile, File toFile, boolean overwrite) throws IOException {
        if( logger.isDebugEnabled() ){
            logger.debug( "START moving file " + fromFile + " to " + toFile );
        }

        if (toFile.equals(fromFile)) {
            if (logger.isDebugEnabled()) {
                logger.debug(String
                        .format("Warning - tried to move a file %s to itself %s - no action taken...",
                                fromFile, toFile));
            }
            return;
        }

        if (toFile.exists() && overwrite && !toFile.delete())
            throw new IOException("Could not delete existing file " + toFile + " to overwrite");

        if (!fromFile.renameTo(toFile)) {
            throw new IOException("Could not move file " + fromFile + " to " + toFile);
        }

        if( logger.isDebugEnabled() ){
            logger.debug( "FINISH moving file " + fromFile + " to " + toFile );
        }
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
