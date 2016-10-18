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

public class NgramMinShouldMatchScenario {

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

        //https://www.elastic.co/guide/en/elasticsearch/guide/current/ngrams-compound-words.html
        /*
        A similar query for “Gesundheit” (health) correctly matches “Welt-gesundheit-sorganisation,” but it also
        matches “Militär-ges-chichte” and “Rindfleischetikettierungsüberwachungsaufgabenübertragungs-ges-etz,” both
        of which also contain the trigram "ges".
         */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "Gesundheit"));
        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 2);

        //https://www.elastic.co/guide/en/elasticsearch/reference/2.3/query-dsl-minimum-should-match.html
        //FIXME "minimum_should_match" not obeyed here -> bug?
        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext", "Gesundheit")
                        .minimumShouldMatch("80%"))
        ;
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        /*
        query in elasticsearch syntax, e.g. for use in head-plugin

        {
            "query": {
                "match": {
                    "fulltext": {
                        "query":                "Gesundheit",
                        "minimum_should_match": "80%"
                    }
                }
            }
        }
         */
    }
}
