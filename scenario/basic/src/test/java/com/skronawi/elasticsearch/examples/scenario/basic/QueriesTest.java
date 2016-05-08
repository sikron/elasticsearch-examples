package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueriesTest extends ScenarioWithEntryMappingBase {

    @Test
    public void searchByQueryString() throws Exception {

        //this searches in the '_all' field

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery("requirement"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0); //does not consider the content.content

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(Entries.test().getIsbn()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //considers the isbn

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(Entries.test().getTitle().toLowerCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //analyzer is default, so case-insensitive

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(Entries.test().getTitle().toUpperCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //analyzer is default, so case-insensitive
    }

    @Test
    public void searchByTerm() throws Exception {

        //this searches in one field explicitly;
        //it exactly searches exactly, so the search-term is not analyzed, and thus the 'term' query is appropriate for
        //'not_analyzed' fields.
        //this is vs. the 'match' query, which is more appropriate for 'analyzed' fields.

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("title", Entries.test().getTitle()));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("title", Entries.test().getTitle().toLowerCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("title", Entries.test().getTitle().toUpperCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
        //search-title is in uppercase, but title was stored analyzed, i.e. in lowercase
    }

    @Test
    public void searchByMatch() throws Exception {

        //this searches like 'query'string', but in a specific field.
        //so the search-term is analyzed and then compared to all analyzed stored terms

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("title", Entries.test().getTitle()));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("title", Entries.test().getTitle().toLowerCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("title", Entries.test().getTitle().toUpperCase()));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }
}
