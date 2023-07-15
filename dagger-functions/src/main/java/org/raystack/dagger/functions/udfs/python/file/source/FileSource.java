package org.raystack.dagger.functions.udfs.python.file.source;

import java.io.IOException;

/**
 * The interface File source.
 */
public interface FileSource {

    /**
     * Get object file byte [ ].
     *
     * @return the byte [ ]
     */
    byte[] getObjectFile() throws IOException;
}
