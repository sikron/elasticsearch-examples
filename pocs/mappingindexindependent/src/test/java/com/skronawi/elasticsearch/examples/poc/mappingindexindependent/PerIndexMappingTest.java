package com.skronawi.elasticsearch.examples.poc.mappingindexindependent;

import com.google.gson.GsonBuilder;
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

public class PerIndexMappingTest {

    public static final String INDEX_1 = "perindexmapping_index_1";
    public static final String INDEX_2 = "perindexmapping_index_2";
    public static final String ENTRY_TYPE = "entry";

    private JestClient client;

    @BeforeClass
    public void setup() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .gson(new GsonBuilder()
                        .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                        .create()) //so that also milliseconds are respected
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
                new IndicesExists.Builder(INDEX_1).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(INDEX_1).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + INDEX_1 + " is not deleted");
            }
        }
        indicesExistResult = client.execute(
                new IndicesExists.Builder(INDEX_2).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(INDEX_2).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + INDEX_2 + " is not deleted");
            }
        }
    }

    @Test
    public void test() throws Exception {

        JestResult index1CreationResult = client.execute(new CreateIndex.Builder(INDEX_1).build());
        Assert.assertTrue(index1CreationResult.isSucceeded());

        JestResult index2CreationResult = client.execute(new CreateIndex.Builder(INDEX_2).build());
        Assert.assertTrue(index2CreationResult.isSucceeded());

        JestResult mapping1CreationResult = client.execute(new PutMapping.Builder(INDEX_1, ENTRY_TYPE,
                resourceAsString("/entry1.json")).build());
        Assert.assertTrue(mapping1CreationResult.isSucceeded());

        JestResult mapping2CreationResult = client.execute(new PutMapping.Builder(INDEX_2, ENTRY_TYPE,
                resourceAsString("/entry2.json")).build());
        Assert.assertTrue(mapping2CreationResult.isSucceeded());

        String entry = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "slim shady")
                .endObject()
                .string();
        DocumentResult indexResult = client.execute(
                new Index.Builder(entry)
                        .index(INDEX_1)
                        .type(ENTRY_TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build());
        Assert.assertTrue(indexResult.isSucceeded());

        entry = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", true)
                .endObject()
                .string();
        indexResult = client.execute(
                new Index.Builder(entry)
                        .index(INDEX_2)
                        .type(ENTRY_TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build());
        Assert.assertTrue(indexResult.isSucceeded());

        //this is not possible in elasticsearch 2.x !! show that the mappings are index-independent by deleting 1 index
//        JestResult deleteMappingResult = client.execute(new DeleteMapping.Builder(INDEX_2, ENTRY_TYPE).build());
//        Assert.assertTrue(deleteMappingResult.isSucceeded());

        String searchQuery = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery()).toString();

        SearchResult searchResult = client.execute(
                new Search.Builder(searchQuery)
                        .addIndex(INDEX_1)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchResult = client.execute(
                new Search.Builder(searchQuery)
                        .addIndex(INDEX_2)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        //now delete index1 and assert that mapping and entry in index2 are still there
        JestResult deleteIndexResult = client.execute(
                new DeleteIndex.Builder(INDEX_1).build());
        Assert.assertTrue(deleteIndexResult.isSucceeded());

        searchResult = client.execute(
                new Search.Builder(searchQuery)
                        .addIndex(INDEX_2)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    private String resourceAsString(String path) throws IOException {
        return new String(IOUtils.toByteArray(getClass().getResourceAsStream(path)));
    }
}
