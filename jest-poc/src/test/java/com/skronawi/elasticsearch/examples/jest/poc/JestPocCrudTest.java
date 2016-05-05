package com.skronawi.elasticsearch.examples.jest.poc;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JestPocCrudTest {

    public static final String JEST_POC_INDEX = "jest_poc_index";
    public static final String SIMPLE_ITEM = "simple_item";

    private JestClient client;

    @BeforeClass
    public void setup() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
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
                new IndicesExists.Builder(JEST_POC_INDEX).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(JEST_POC_INDEX).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + JEST_POC_INDEX + " is not deleted");
            }
        }
    }

    @Test
    public void createGet() throws Exception {

        SimpleItem dummy = SimpleItem.dummy();

        DocumentResult indexResult = client.execute(
                new Index.Builder(dummy)
                        .index(JEST_POC_INDEX)
                        .type(SIMPLE_ITEM)
                        .build()
        );

        Assert.assertNotNull(indexResult);
        Assert.assertEquals(indexResult.getResponseCode(), 201); //CREATED
        Assert.assertEquals(indexResult.getType(), SIMPLE_ITEM);
        Assert.assertEquals(indexResult.getIndex(), JEST_POC_INDEX);
        Assert.assertNotNull(indexResult.getId());

        DocumentResult getResult = client.execute(
                new Get.Builder(JEST_POC_INDEX, indexResult.getId())
                        .type(SIMPLE_ITEM)
                        .build()
        );
        Assert.assertNotNull(getResult);
        Assert.assertEquals(getResult.getResponseCode(), 200);
        Assert.assertEquals(getResult.getSourceAsObject(SimpleItem.class), dummy);
    }
}
