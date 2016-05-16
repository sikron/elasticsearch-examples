package com.skronawi.elasticsearch.examples.poc.multi;

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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MultiLangCopyToScenario {

    public static final String INDEX = "multifield_poc_index";
    public static final String TYPE = "multi-lang-entry";

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
    public void testNoAttachment() throws Exception {

        //create the index
        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .put("index.mapping.attachment.detect_language", true) //expensive
                .build();
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(INDEX)
                .settings(indexSettings)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        /*
        the copy_to fields are not real fields in the stored document, they are only virtual fields to be searched.
        they exist in the inverted index only.
         */

        PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,
                new String(IOUtils.toByteArray(
                        getClass().getResourceAsStream("/multi-lang-entry_no-attach.json")))).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        String body = XContentFactory.jsonBuilder()
                .startObject()
//                .field("file", "Ganz viele Aufträge")
                .field("file", "Jede Menge Mängel")
                .field("title", "my title")
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
        'Aufträge' is analyzed to 'auftrag' by german and 'aufträg' by english analyzer
         */
        /*
        'Mängel' is analyzed to 'mangel' by german and 'mängel' by english analyzer
         */

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
//                .query(QueryBuilders.matchQuery("fulltext_de", "Auftrag"))
                .query(QueryBuilders.matchQuery("fulltext_de", "Mangel"))
                ;
        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
//                .query(QueryBuilders.matchQuery("fulltext_en", "Auftrag"))
                .query(QueryBuilders.matchQuery("fulltext_en", "Mangel"))
                ;
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }

    //FIXME currently failing as the 'copy_to' within the attachment field is not working
    //see  https://github.com/elastic/elasticsearch/issues/14946 <- closed
    //clarified here:  https://github.com/elastic/elasticsearch/issues/18361
    //does not work with attachment plugin
    @Test
    public void testAttachment() throws Exception {

        //create the index
        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .put("index.mapping.attachment.detect_language", true) //expensive
                .build();
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(INDEX)
                .settings(indexSettings)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,
                new String(IOUtils.toByteArray(
                        getClass().getResourceAsStream("/multi-lang-entry_attach_no-multifield.json")))).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        //read in a pdf, base64-encode it and index it. also refresh!
        byte[] base64Bytes = Base64.encodeBase64(IOUtils.toByteArray(
                getClass().getResourceAsStream("/muster-vertrag.pdf"))
        );

        String body = XContentFactory.jsonBuilder()
                .startObject()
                .field("file", new String(base64Bytes)) //must be a string !
                .field("title", "muster-vertrag.pdf")
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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext_de", "Mangel"))
                ;
        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("fulltext_en", "Mangel"))
        ;
        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 0);
    }
}
