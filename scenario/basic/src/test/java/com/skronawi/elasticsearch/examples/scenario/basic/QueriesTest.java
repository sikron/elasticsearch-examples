package com.skronawi.elasticsearch.examples.scenario.basic;

import com.skronawi.elasticsearch.examples.scenario.basic.util.Entries;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.Date;

public class QueriesTest extends ScenarioWithMappingAndEntryBase {

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

    @Test
    public void searchByMultiMatch() throws Exception{

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.multiMatchQuery(Entries.test().getTitle(), "title",
                        "content.content", "isbn"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchByPrefix() throws Exception{

        String title = Entries.test().getTitle();
        String prefix = title.substring(0, title.lastIndexOf("."));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.prefixQuery("title", prefix));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchByPrefixPhraseAndInContent() throws Exception{

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchPhrasePrefixQuery("content.content", "The pdf995 su"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchWithBoolQuery() throws Exception{

        Entry entry = Entries.test();

        MatchQueryBuilder titleQuery = QueryBuilders.matchQuery("title", entry.getTitle().toUpperCase());

        Date fromDate = new DateTime(entry.getReleaseDate()).minusDays(1).toDate();
        Date toDate = new DateTime(entry.getReleaseDate()).plusDays(1).toDate();
        RangeQueryBuilder releaseDateQuery = QueryBuilders.rangeQuery("releaseDate").from(fromDate).to(toDate);

        TermQueryBuilder contentQuery = QueryBuilders.termQuery("content.content", "links");
        TermQueryBuilder notExistingContentQuery = QueryBuilders.termQuery("content.content", "foobar-links");

        //now play around

        SearchSourceBuilder titleReleaseDateQuery = new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(titleQuery).must(releaseDateQuery));
        SearchResult searchResult = this.search.search(titleReleaseDateQuery.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        SearchSourceBuilder titleNotReleaseDateQuery = new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(titleQuery).mustNot(releaseDateQuery));
        searchResult = this.search.search(titleNotReleaseDateQuery.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);

        SearchSourceBuilder titleShouldContentQuery = new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(titleQuery).should(contentQuery).should(notExistingContentQuery));
        searchResult = this.search.search(titleShouldContentQuery.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        SearchSourceBuilder titleShouldContentBothQuery = new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(titleQuery).should(contentQuery).should(notExistingContentQuery)
                        .minimumNumberShouldMatch(2));
        searchResult = this.search.search(titleShouldContentBothQuery.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }
}
