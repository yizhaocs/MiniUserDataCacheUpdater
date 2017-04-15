package com.yizhao.miniudcu.clog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Created by yzhao on 4/15/17.
 */
public abstract class AbstractLoggable implements Loggable {

    private boolean generatePrimaryKey = true;
    private boolean generateModificationTS = true;
    private static final String SEPARATOR = "|";
    private static final Pattern PATTERN_BACKSLASH = Pattern.compile("\\\\");
    private static final Pattern PATTERN_PIPE = Pattern.compile("\\|");
    private static final Pattern PATTERN_NEW_LINE = Pattern
            .compile("\\r|\\r\\n|\\n");

    // private static final String ENCLOSED_BY = "";

    /*
     * (non-Javadoc)
     *
     * @see com.opinmind.clog.Loggable#addLoggableAttribute(java.lang.Object)
     */
    public void addAttribute(Object attribute) {
    }

    public void addAttributes(Object... attrs) {
    }

    /*
     * (non-Javadoc)
     *
     * @see com.opinmind.clog.Loggable#addAttributes(java.util.List)
     */
    public void addAttributes(List<Object> attribute) {
    }

    protected String doToCsv(Collection<Object> attributes1,
                             Collection<Object> attributes2, boolean generatePrimaryKey) {
        Collection<Object> finalAttributes = new ArrayList<Object>();

        if (attributes1 != null) {
            finalAttributes.addAll(attributes1);
        }
        if (attributes2 != null) {
            finalAttributes.addAll(attributes2);
        }
        return doToCsv(finalAttributes, SEPARATOR, generatePrimaryKey);
    }

    protected String doToCsv(Collection<Object> attributes1,
                             Collection<Object> attributes2) {
        return doToCsv(attributes1, attributes2, generatePrimaryKey);
    }

    /**
     * The method to convert to csv file.
     *
     * @param attributes
     * @param separator
     * @param generatePrimaryKey
     * @return
     */
    private final String doToCsv(Collection<Object> attributes,
                                 String separator, boolean generatePrimaryKey) {

        if (attributes != null && attributes.size() > 0) {
            StringBuilder sb = new StringBuilder();
            if(generateModificationTS){
                sb.append(new Date());
            }
            if (generatePrimaryKey) {
                UUID id = UUID.randomUUID();
                sb.append(separator);
                sb.append(id.getMostSignificantBits());
                sb.append(separator);
                sb.append(id.getLeastSignificantBits());
            }
            boolean first = true;
            for (Object attr : attributes) {
                if(generateModificationTS){
                    sb.append(separator);
                }
                else{
                    if(!first){
                        sb.append(separator);
                    }
                    first = false;
                }
                String finalAttr = "NULL";
                if (attr == null) {
                    finalAttr = "NULL"; // 2011-09-01: bug 4434, use NULL to
                    // represent null value
                } else if ("NULL".equalsIgnoreCase(attr.toString())) {
                    // This is the extremely rare case where we want to save a literal string "NULL" rather than just a null value
                    //   ( i'm not aware of any actual desired case in prod, but theoretically possible )
                    // bug 19799: make sure its case in-sensitive check
                    //
                    //   Need to escape for the sake of nzload
                    //
                    finalAttr = "\\" + attr.toString();
                } else {
                    finalAttr = PATTERN_BACKSLASH.matcher(attr.toString())
                            .replaceAll("\\\\\\\\");
                    finalAttr = PATTERN_PIPE.matcher(finalAttr)
                            .replaceAll("\\\\|");
                    finalAttr = PATTERN_NEW_LINE
                            .matcher(finalAttr).replaceAll(" ");
                    finalAttr = finalAttr.trim();
                    /*
                     * finalAttr = attr.toString().replaceAll("\\\\",
                     * "\\\\\\\\").replaceAll("\\|", "\\\\|") .replaceAll("\\r",
                     * " ").replaceAll("\\n", " ").trim();
                     */
                }
                sb.append(finalAttr);
            }
            return sb.toString();
        }
        return "";
    }

    public String toHdfsCsv(Collection<Object> permanentAttributes) {
        return toCsv(permanentAttributes);
    }

    public boolean isGeneratePrimaryKey() {
        return generatePrimaryKey;
    }

    public void setGeneratePrimaryKey(boolean generatePrimaryKey) {
        this.generatePrimaryKey = generatePrimaryKey;
    }

    public boolean isGenerateModificationTS() {
        return generateModificationTS;
    }

    public void setGenerateModificationTS(boolean generateModificationTS) {
        this.generateModificationTS = generateModificationTS;
    }


}