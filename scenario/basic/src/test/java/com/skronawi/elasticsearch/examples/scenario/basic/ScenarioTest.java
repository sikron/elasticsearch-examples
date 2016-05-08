package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import com.skronawi.elasticsearch.examples.scenario.basic.util.Mappings;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ScenarioTest extends ScenarioBase {

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
                .query(QueryBuilders.matchQuery("content.content", "bookmark"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "mookbark"));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }
}
