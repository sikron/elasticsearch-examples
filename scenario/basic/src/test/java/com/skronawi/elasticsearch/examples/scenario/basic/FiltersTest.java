package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/*
FilterBuilder etc. is deprecated in ES 2.0. Instead use a bool query with must clause, see
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-filtered-query.html#_filtering_without_a_query
 */
public class FiltersTest extends ScenarioWithMappingAndEntryBase {

    @Test
    public void filterByQueryString() throws Exception {

        //this searches in the '_all' field

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.queryStringQuery("requirement")));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0); //does not consider the content.content

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.queryStringQuery(Entries.test().getIsbn())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //considers the isbn

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.queryStringQuery(Entries.test().getTitle().toLowerCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //analyzer is default, so case-insensitive

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.queryStringQuery(Entries.test().getTitle().toUpperCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1); //analyzer is default, so case-insensitive
    }

    @Test
    public void filterByTerm() throws Exception {

        //this searches in one field explicitly;
        //it exactly searches exactly, so the search-term is not analyzed, and thus the 'term' query is appropriate for
        //'not_analyzed' fields.
        //this is vs. the 'match' query, which is more appropriate for 'analyzed' fields.

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("title", Entries.test().getTitle())));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("title", Entries.test().getTitle().toLowerCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("title", Entries.test().getTitle().toUpperCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
        //search-title is in uppercase, but title was stored analyzed, i.e. in lowercase
    }

    @Test
    public void filterByMatch() throws Exception {

        //this searches like 'query'string', but in a specific field.
        //so the search-term is analyzed and then compared to all analyzed stored terms

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.matchQuery("title", Entries.test().getTitle())));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.matchQuery("title", Entries.test().getTitle().toLowerCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.matchQuery("title", Entries.test().getTitle().toUpperCase())));
        searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }
}
