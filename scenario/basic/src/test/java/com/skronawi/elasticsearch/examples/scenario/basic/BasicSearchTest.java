package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BasicSearchTest extends BasicScenarioBase {

    @BeforeClass
    public void indexAnEntry() throws Exception {
        JestResult createMappingResult = admin.createMapping(INDEX_NAME, MAPPING_NAME, Mappings.entry());
        Assert.assertTrue(createMappingResult.isSucceeded());

        DocumentResult createEntryResult = index.create(Entries.test().serialize(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(createEntryResult.isSucceeded());

        admin.refresh(INDEX_NAME);

        DocumentResult getResult = index.get(createEntryResult.getId(), INDEX_NAME, MAPPING_NAME);
        Assert.assertTrue(getResult.isSucceeded());
    }

    @Test
    public void searchEquals() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "manifestation"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchEqualsIgnoreCase() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "ManiFESTatiOn"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchNotEquals() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "fanimestation"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }

    @Test
    public void searchPart() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.wildcardQuery("content.content", "*festat*"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchSingularOfExistingPlural() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "requirements"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test(enabled = false)  //does not work, maybe explicit analysis needed ?
    public void searchPluralOfExistingSingular() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "modes"));
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    @Test
    public void searchAlsoByMetadata() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content.content", "manifestation"))
                .query(QueryBuilders.matchQuery("isbn", "5678"))
                ;
        SearchResult searchResult = this.search.search(searchSourceBuilder.toString(), INDEX_NAME);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }
}
