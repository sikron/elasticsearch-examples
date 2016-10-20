package com.skronawi.elasticsearch.examples.poc.ngram;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
Here a ngram tokenizer is used, which matches for all 3-ngrams of the search-term.
So only documents are returned, which have all the 3-ngrams of the search-term.

see https://www.elastic.co/guide/en/elasticsearch/reference/2.4/analysis-ngram-tokenizer.html
 */
public class NgramTokenizerMinShouldMatchScenario {

    public static final String INDEX = "ngram_poc_index";
    public static final String TYPE = "ngram-entry";

    private JestClient client;

    @BeforeClass
    public void setup() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());
        client = factory.getObject();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        if (client == null) {
            return;
        }
        client.shutdownClient();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (client == null) {
            return;
        }
        JestResult indicesExistResult = client.execute(
                new IndicesExists.Builder(INDEX).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(INDEX).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + INDEX + " is not deleted");
            }
        }
    }

    @Test
    public void test() throws Exception {

        //use the ngram-tokenizer!!!!
        String indexSettingsJson = new String(IOUtils.toByteArray(
                getClass().getResourceAsStream("/index-settings_ngram-tokenizer.json")));

        //create the index
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(INDEX)
                .settings(indexSettingsJson)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,
                new String(IOUtils.toByteArray(
                        getClass().getResourceAsStream("/ngram-entry.json")))).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        String body = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "1st.txt")
                .field("content", "Weltgesundheitsorganisation")
                .endObject().string();
        DocumentResult indexResult = client.execute(
                new Index.Builder(body)
                        .index(INDEX)
                        .type(TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build()
        );
        Assert.assertNotNull(indexResult);
        Assert.assertTrue(indexResult.isSucceeded());

        body = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "2nd.txt")
                .field("content", "Militärgeschichte")
                .endObject().string();
        indexResult = client.execute(
                new Index.Builder(body)
                        .index(INDEX)
                        .type(TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build()
        );
        Assert.assertNotNull(indexResult);
        Assert.assertTrue(indexResult.isSucceeded());

        /*
        here "ges" from "Gesundheit" will not result in "Militärgeschichte" being in the result set.
        only the documents, which have all the 3-ngrams of "gesundheit" will match
        -> only the one with "Weltgesundheitsorganisation"
        the "minimum_should_match" has actually no effect here
         */

//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
//                .query(QueryBuilders.matchQuery("fulltext", "Gesundheit"));
//        SearchResult searchResult = client.execute(
//                new Search.Builder(searchSourceBuilder.toString())
//                        .addIndex(INDEX)
//                        .build());
//        Assert.assertNotNull(searchResult);
//        Assert.assertEquals(searchResult.getTotal().intValue(), 2);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "Gesundheit")
                        .minimumShouldMatch("1%"));
        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "Gesundheit")
                        .minimumShouldMatch("80%"));
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "undhei"));
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "ges"));
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 2);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "ge"));
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }
}
