package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicScenarioTest extends BasicScenarioBase {

    @Test
    public void test() throws Exception {

        JestResult createMappingResult = admin.createMapping(INDEX_NAME, MAPPING_NAME, Mappings.entry());
        Assert.assertTrue(createMappingResult.isSucceeded());

        DocumentResult createEntryResult = index.create(Entries.test().serialize(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(createEntryResult.isSucceeded());

        admin.refresh(INDEX_NAME);

        DocumentResult getResult = index.get(createEntryResult.getId(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(getResult.isSucceeded());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "bookmark")); //as "Bookmarks" is contained
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "mookbark")); //as "Bookmarks" is contained
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }

    /*
    - search by part: "mark"
    - search by plural of contained singular: "workflows"
    - search combined: "bookmark" & isbn -> pos & neg
    - search should NOT return the content, only metadata or specified fields
    - search for german full-text
    */
}
