package io.odpf.dagger.functions.udfs.scalar.dart.store.gcs;


import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.functions.exceptions.BucketDoesNotExistException;
import io.odpf.dagger.functions.exceptions.TagDoesNotExistException;
import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GcsDataStoreTest {


    private final String defaultListName = "listName";

    private final String defaultMapName = "mapName";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private GcsDataStore gcsDataStore;
    private List<String> listContent;
    private Map<String, String> mapContent;
    private GcsClient gcsClient;
    private MeterStatsManager meterStatsManager;

    @Before
    public void setUp() {
        gcsDataStore = mock(GcsDataStore.class);
        gcsClient = mock(GcsClient.class);
        meterStatsManager = mock(MeterStatsManager.class);
        when(gcsDataStore.getSet(anyString())).thenCallRealMethod();
        when(gcsDataStore.getMap(anyString())).thenCallRealMethod();
        when(gcsDataStore.getGcsClient()).thenReturn(gcsClient);
        when(gcsDataStore.getMeterStatsManager()).thenReturn(meterStatsManager);
        listContent = Arrays.asList("listContent");
        mapContent = Collections.singletonMap("key", "value");

    }


    @Test
    public void shouldGetExistingListGivenName() {
        String jsonData = " { \"data\" : [ \"listContent\" ] } ";

        when(gcsClient.fetchJsonData(any(), any(), any(), anyString())).thenReturn(jsonData);
        SetCache setCache = new SetCache(new HashSet<>(listContent));
        assertEquals(setCache, gcsDataStore.getSet(defaultListName));
    }

    @Test
    public void shouldThrowTagDoesNotExistWhenListIsNotThere() {
        thrown.expect(TagDoesNotExistException.class);
        thrown.expectMessage("Could not find the content in gcs for invalidListName");


        when(gcsClient.fetchJsonData(any(), any(), any(), anyString())).thenThrow(new TagDoesNotExistException("Could not find the content in gcs for invalidListName"));

        gcsDataStore.getSet("invalidListName");
    }

    @Test
    public void shouldThrowBucketDoesNotExistWhenBucketIsNotThere() {
        thrown.expect(BucketDoesNotExistException.class);
        thrown.expectMessage("Could not find the bucket in gcs for invalidListName");


        when(gcsClient.fetchJsonData(any(), any(), any(), anyString())).thenThrow(new BucketDoesNotExistException("Could not find the bucket in gcs for invalidListName"));

        gcsDataStore.getSet("invalidListName");
    }


    @Test
    public void shouldGetExistingMapGivenName() {

        String jsonData = " { \"key\" :  \"value\"  } ";
        when(gcsClient.fetchJsonData(any(), any(), any(), anyString())).thenReturn(jsonData);
        MapCache mapCache = new MapCache(new HashMap<>(mapContent));

        assertEquals(mapCache, gcsDataStore.getMap(defaultMapName));
    }

    @Test
    public void shouldThrowTagDoesNotExistWhenMapIsNotThere() {
        thrown.expect(TagDoesNotExistException.class);
        thrown.expectMessage("Could not find the content in gcs for invalidMapName");

        when(gcsClient.fetchJsonData(any(), any(), any(), anyString())).thenThrow(new TagDoesNotExistException("Could not find the content in gcs for invalidMapName"));

        gcsDataStore.getSet("invalidMapName");
    }


}
