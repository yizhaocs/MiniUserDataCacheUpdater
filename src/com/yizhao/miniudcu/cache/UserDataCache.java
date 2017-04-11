package com.yizhao.miniudcu.cache;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface UserDataCache {
    public Map<Integer, Integer> getCache(long cookieId) throws Exception;

    public void setCache(long cookieId, Map<Integer, Integer> data) throws Exception;

    public void destroy();
}