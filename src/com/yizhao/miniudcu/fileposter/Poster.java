package com.yizhao.miniudcu.fileposter;

import java.io.File;
import java.util.Collection;

/**
 * Created by yzhao on 4/15/17.
 */
public interface Poster {

    public Collection<File> getPendingFiles() throws Exception;

    public void donePosting(File file) throws Exception;

}