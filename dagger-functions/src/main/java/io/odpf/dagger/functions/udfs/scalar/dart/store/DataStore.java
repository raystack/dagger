package io.odpf.dagger.functions.udfs.scalar.dart.store;

import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;

public interface DataStore {
    SetCache getSet(String setName);

    MapCache getMap(String mapName);
}
