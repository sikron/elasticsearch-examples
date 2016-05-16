package com.skronawi.elasticsearch.examples.poc.storeindex;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
http://stackoverflow.com/questions/15299799/elasticsearch-impact-of-setting-a-not-analyzed-field-as-storeyes

You can have fields that you only want to search on, and never show: indexed and not stored (default in lucene).
You can have fields that you want to search on and also retrieve: indexed and stored.
You can have fields that you don't want to search on, but you do want to retrieve to show them.

You end up having two copies of the same data in lucene only if you decide to store a field (store:yes in the mapping),
since elasticsearch keeps that same content within the json _source, but this doesn't have anything to do with the fact
that you're indexing or analyzing the field.

------------------------------------------------------------------------------------------------------------------------

http://stackoverflow.com/questions/17103047/why-do-i-need-storeyes-in-elasticsearch?rq=1
 */
public class StoreIndexTest {

    public static final String INDEX = "storedindexed_poc_index";
    public static final String TYPE = "stored-indexed-entry";

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

        //create the index
        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .build();
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(INDEX)
                .settings(indexSettings)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,
                new String(IOUtils.toByteArray(
                        getClass().getResourceAsStream("/stored-indexed-entry.json")))).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        byte[] base64Bytes = Base64.encodeBase64(IOUtils.toByteArray(
                getClass().getResourceAsStream("/muster-vertrag.pdf"))
        );

        String body = XContentFactory.jsonBuilder()
                .startObject()
                .field("no-store-file", new String(base64Bytes))
                .field("not_stored-indexed-string", "funny")
                .field("stored-indexed-string", "cloudy")
                .field("stored-not_indexed-string", "sunny")
                .field("not_stored-not_indexed-string", "noisy")
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

        /*
        only the indexed fields can be searched.
        also the non-stored fields can be retrieved as they are automatically stored in the "_source" field by elasticsearch.
         */
    }
}
