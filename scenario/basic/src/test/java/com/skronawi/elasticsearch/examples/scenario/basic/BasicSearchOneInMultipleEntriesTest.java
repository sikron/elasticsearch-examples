package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class BasicSearchOneInMultipleEntriesTest extends BasicScenarioBase {

    @BeforeClass
    public void index() throws Exception {
        JestResult createMappingResult = admin.createMapping(INDEX_NAME, MAPPING_NAME, Mappings.entry());
        Assert.assertTrue(createMappingResult.isSucceeded());

        Arrays.asList(Entries.legal5(), Entries.lorem(), Entries.test()).forEach(entry -> {
            DocumentResult createEntryResult = null;
            try {
                createEntryResult = index.create(entry.serialize(), INDEX_NAME, MAPPING_NAME);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
            Assert.assertTrue(createEntryResult.isSucceeded());
        });

        admin.refresh(INDEX_NAME);
    }

    @Test
    public void searchOne() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "manifestation"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }
}
