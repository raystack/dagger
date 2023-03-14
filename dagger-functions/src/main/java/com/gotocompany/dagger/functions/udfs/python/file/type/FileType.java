package com.gotocompany.dagger.functions.udfs.python.file.type;

import java.io.IOException;
import java.util.List;

/**
 * The interface File type.
 */
public interface FileType {

    /**
     * Gets file names.
     *
     * @return the file names
     */
    List<String> getFileNames() throws IOException;
}
