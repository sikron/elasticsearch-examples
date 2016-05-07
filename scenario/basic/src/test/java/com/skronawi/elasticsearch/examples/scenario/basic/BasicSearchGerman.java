package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BasicSearchGerman extends BasicScenarioBase {

    @BeforeClass
    public void indexAnEntry() throws Exception {
        JestResult createMappingResult = admin.createMapping(INDEX_NAME, MAPPING_NAME, Mappings.entry());
        Assert.assertTrue(createMappingResult.isSucceeded());

        DocumentResult createEntryResult = index.create(Entries.germanContract().serialize(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(createEntryResult.isSucceeded());

        admin.refresh(INDEX_NAME);

        DocumentResult getResult = index.get(createEntryResult.getId(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(getResult.isSucceeded());
    }

    @Test
    public void searchEquals() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .field("content.language")
                .query(QueryBuilders.matchQuery("content.content", "diesbezüglichen"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
        Assert.assertTrue(searchResult.getJsonString().contains("\"de\""));
    }
}
