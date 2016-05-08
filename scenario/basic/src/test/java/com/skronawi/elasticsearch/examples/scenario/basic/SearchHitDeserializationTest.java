package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchHitDeserializationTest extends ScenarioWithEntryMappingBase {

    @Test
    public void testHit() throws Exception{

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "manifestation"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        //see http://jtruty.github.io/programming/2015/04/03/elasticsearch-http-queries-with-jest.html
        //and gson date converter in Search.java
        Entry foundEntry = searchResult.getFirstHit(Entry.class).source;
        Assert.assertEquals(foundEntry, Entries.test());
    }
}
