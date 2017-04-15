package com.yizhao.miniudcu.clog;

import java.util.Collection;
import java.util.List;

/**
 * Loggable interface represents a bean that can be converted into a "csv"
 * string which gets logged. If the csv is destined for Netezza tables, the
 * columns need to be in the order of Netezza loader configuration.
 *
 */
public interface Loggable {

    public void addAttribute(Object attribute);

    public void addAttributes(List<Object> attribute);

    public void addAttributes(Object... attrs);

    public String toCsv(Collection<Object> permanentAttributes);

}