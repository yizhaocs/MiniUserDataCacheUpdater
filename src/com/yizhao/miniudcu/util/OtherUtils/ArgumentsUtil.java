package com.yizhao.miniudcu.util.OtherUtils;

import java.util.Map;

/**
 * Created by yzhao on 4/15/17.
 */
public class ArgumentsUtil {
    /**
     * Validates passed arguments. Null arguments or zero-length {@link String}
     * objects result in NullPointerException or IllegalArgumentsException being
     * thrown respectively.
     *
     * @param objects a varargs list of arguments to be validated.
     * @throws NullPointerException if a null object is passed as an argument.
     * @throws IllegalArgumentException if a zero-length {@link String} is passed
     *           as an argument.
     */
    public static void validateMandatory(Object... objects) throws NullPointerException,
            IllegalArgumentException {
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] == null)
                throw new NullPointerException("The parameter number " + (i + 1) + " is null");
            if (objects[i] instanceof String && ((String) objects[i]).length() == 0)
                throw new IllegalArgumentException("The String parameter number " + i
                        + " is null or of zero length");
        }
    }

    public static void validateMandatory(Map<String, String> errorMessages, Object... objects)
            throws NullPointerException, IllegalArgumentException {
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] == null) {
                String iStr = i + "";
                String message = errorMessages != null && errorMessages.get(iStr) != null ? errorMessages
                        .get(iStr) : ("The parameter number " + (i + 1) + " is null");
                throw new NullPointerException(message);
            }
            if (objects[i] instanceof String && ((String) objects[i]).length() == 0) {
                String iStr = i + "";
                String message = errorMessages != null && errorMessages.get(iStr) != null ? errorMessages
                        .get(iStr) : ("The String parameter number " + i + " is null or of zero length");
                throw new IllegalArgumentException(message);
            }
        }
    }
}
