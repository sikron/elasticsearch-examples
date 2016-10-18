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

import java.io.IOException;

public class NgramScenario {

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

        String indexSettingsJson = new String(IOUtils.toByteArray(
                getClass().getResourceAsStream("/index-settings.json")));

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
                .field("name", "my-file.txt")
                .field("content", "This is a standard text. It has a lot of words, some of which are stop words actually. " +
                        "Also some duplicate words exist, foo bar wording")
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

        searchAndExpectHitNumber("fulltext", "also", 1); //lowercase, uppercase transparent
        searchAndExpectHitNumber("fulltext", "Also", 1); //...
        searchAndExpectHitNumber("fulltext", "file", 1); //also filename is included
        searchAndExpectHitNumber("fulltext", "andar", 1); //ngrams work
        searchAndExpectHitNumber("fulltext", "word", 1);
        searchAndExpectHitNumber("fulltext", "wording", 1);
        searchAndExpectHitNumber("fulltext", "my-file", 1);
        searchAndExpectHitNumber("fulltext", "my-file.txt", 1);
        searchAndExpectHitNumber("fulltext", "It", 0); //too short
        searchAndExpectHitNumber("fulltext", "to", 0); //too short
    }

    private void searchAndExpectHitNumber(String fieldname, String value, int number) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(fieldname, value));
        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), number);
    }
}
