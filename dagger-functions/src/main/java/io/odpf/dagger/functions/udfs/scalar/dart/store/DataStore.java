package io.odpf.dagger.functions.udfs.scalar.dart.store;

import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;

/**
 * The interface Data store.
 */
public interface DataStore {
    /**
     * Gets set.
     *
     * @param setName the set name
     * @return the set
     */
    SetCache getSet(String setName);

    /**
     * Gets map.
     *
     * @param mapName the map name
     * @return the map
     */
    MapCache getMap(String mapName);
}
