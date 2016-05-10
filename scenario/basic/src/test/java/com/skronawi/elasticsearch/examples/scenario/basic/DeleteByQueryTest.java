package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import com.skronawi.elasticsearch.examples.scenario.basic.util.EntryWithId;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteByQueryTest extends ScenarioWithMappingAndEntryBase {

    @Test
    public void test() throws Exception {

        //get the entry to have its id to get it later by id
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        String id = searchResult.getFirstHit(EntryWithId.class).source.getId();
        Assert.assertNotNull(id);

        //delete the entry
        String deleteQuery = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("title", Entries.test().getTitle())).toString();
        JestResult deleteByQueryResult = index.deleteByQuery(deleteQuery, INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(deleteByQueryResult.isSucceeded());

        //get it by id and check, that it does not exist any more
        DocumentResult getResult = index.get(id, INDEX_NAME, MAPPING_NAME);
        Assert.assertFalse(getResult.isSucceeded());
    }
}
