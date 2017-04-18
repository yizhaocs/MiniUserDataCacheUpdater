package com.yizhao.miniudcu.util.FileUtils;

import com.yizhao.miniudcu.util.ArgumentUtils.ArgumentsUtil;

import java.io.File;
import java.util.Collection;
import java.util.Random;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileVerifyUtil {

    /**
     * Verify that the given files are all readable.
     *
     * @param files - files to be checked.
     * @throws IllegalArgumentException, if one or more files are not readable.
     */
    public static void verifyFilesAreReadable(File[] files) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory((Object[]) files);
        for (File file : files) {
            verifyFileIsReadable(file);
        }
    }

    public static long nextLong(Random random, long n) {
        long result = 0;
        result = (long) (random.nextDouble() * n);
        return result;
    }

    /**
     * Verify that the given files are all readable.
     *
     * @param files - files to be checked.
     * @throws IllegalArgumentException, if one or more files are not readable.
     */
    public static void verifyFilesAreReadable(Collection<File> files) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(files);
        for (File file : files) {
            verifyFileIsReadable(file);
        }
    }

    /**
     * Verify that the given files are all executable.
     *
     * @param files - files to be checked.
     * @throws IllegalArgumentException, if one or more files are not executable.
     */
    public static void verifyFilesAreExecutable(File[] files) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory((Object[])files);
        for (File file : files) {
            verifyFileIsExecutable(file);
        }
    }


    /**
     * Verify that the given file is readable.
     *
     * @param fileName - the absolute file name.
     * @throws IllegalArgumentException, if the file is not readable.
     */
    public static void verifyFileIsReadable(String fileName) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(fileName);
        verifyFileIsReadable(new File(fileName));
    }

    /**
     * Verify that the given file is readable.
     *
     * @param file - the file.
     * @throws IllegalArgumentException, if the file is not readable.
     */
    public static void verifyFileIsReadable(File file) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(file);
        if (!file.canRead()) {
            throw new IllegalArgumentException("Can not read file: " + file.getAbsolutePath());
        }
    }

    /**
     * Verify that the given file is executable.
     *
     * @param file - the file.
     * @throws IllegalArgumentException, if the file is not executable.
     */
    public static void verifyFileIsExecutable(File file) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(file);
        if (!file.canExecute()) {
            throw new IllegalArgumentException("Can not execute file: " + file.getAbsolutePath());
        }
    }

    /**
     * Verify that the given directory is readable.
     *
     * @param dirName - the absolute name of the dirctory.
     * @throws IllegalArgumentException, if the file name is not a directory or is
     *           not readable.
     */
    public static void verifyIsReadableDirectory(String dirName) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(dirName);
        File file = new File(dirName);

        if (!file.isDirectory())
            throw new IllegalArgumentException("The file " + dirName + " is not a directory.");
        if (!file.canRead())
            throw new IllegalArgumentException("Directory " + dirName + " is not readable.");
    }

    public static void verifyIsReadableDirectory(File dir) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(dir);
        verifyIsReadableDirectory(dir.getAbsolutePath());
    }

    /**
     * Verify given directory is writable.
     *
     * @param dirName - directory name.
     * @throws IllegalArgumentException, if directory is not writable.
     */
    public static void verifyIsWritableDirectory(String dirName) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(dirName);
        verifyIsWritableDirectory(new File(dirName));
    }

    public static void verifyIsWritableFile(String fileName) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(fileName);
        verifyIsWritableFile(new File(fileName));
    }

    public static void verifyIsWritableFile(File file) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(file);
        if (!file.isFile())
            throw new IllegalArgumentException("The file " + file + " is not a file.");
        if (!file.canWrite())
            throw new IllegalArgumentException("File " + file + " is not writable.");
    }

    /**
     * Verify all directories are writable.
     *
     * @param directories - directories that should be writable.
     */
    public static void verifyDirectoriesAreReadable(String[] directories) {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (String dirName : directories) {
            verifyFileIsReadable(dirName);
        }
    }

    /**
     * Verify all passed directories are readable.
     *
     * @param directories - directories that should be readable.
     */
    public static void verifyDirectoriesAreReadable(File[] directories) {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (File directory : directories) {
            verifyFileIsReadable(directory);
        }
    }

    /**
     * Verify all passed directories are writable.
     *
     * @param directories - directories that should be readable.
     */
    public static void verifyDirectoriesAreWriteable(File[] directories) {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (File directory : directories) {
            verifyIsWritableDirectory(directory);
        }
    }

    /**
     * Verify all passed directories are writable.
     *
     * @param directories - directories that should be readable.
     */
    public static void verifyDirectoriesAreWriteable(String[] directories) {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (String directory : directories) {
            verifyIsWritableDirectory(directory);
        }
    }

    /**
     * Verify given directory is writable.
     * @param file
     * @throws IllegalArgumentException
     */
    public static void verifyIsWritableDirectory(File file) throws IllegalArgumentException {
        ArgumentsUtil.validateMandatory(file);
        if (!file.isDirectory())
            throw new IllegalArgumentException("The file " + file + " is not a directory.");
        if (!file.canWrite())
            throw new IllegalArgumentException("Directory " + file + " is not writable.");
    }
}
