package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import com.skronawi.elasticsearch.examples.scenario.basic.util.Mappings;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

public abstract class ScenarioWithMappingAndEntryBase extends ScenarioBase {

    @BeforeClass
    public void indexAnEntry() throws Exception {
        JestResult createMappingResult = admin.createMapping(INDEX_NAME, MAPPING_NAME, Mappings.entry());
        Assert.assertTrue(createMappingResult.isSucceeded());

        DocumentResult createEntryResult = index.create(Entries.test().serialize(),
                INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(createEntryResult.isSucceeded());

        admin.refresh(INDEX_NAME);

        DocumentResult getResult = index.get(createEntryResult.getId(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(getResult.isSucceeded());
    }
}
