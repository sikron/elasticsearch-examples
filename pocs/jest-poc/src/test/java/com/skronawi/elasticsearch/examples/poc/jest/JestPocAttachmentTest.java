package com.skronawi.elasticsearch.examples.poc.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
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
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
https://github.com/elastic/elasticsearch-mapper-attachments
https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/mapper-attachments.html
https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/mapper-attachments-helloworld.html
https://hustbill.wordpress.com/2015/09/11/full-text-search-by-elasticsearch-mapper-attachments-in-pdf-format/
https://books.google.com/books?id=fF5uAgAAQBAJ&pg=PT151&lpg=PT151&dq=bin/plugin+-install+elasticsearch/elasticsearch-mapper-attachments&source=bl&ots=DD5AMfxuZw&sig=52f-co2Any2SQ-1bbkGx7LOV_8o&hl=en&sa=X&ved=0ahUKEwi9z8G60sLMAhVD1GMKHXDjCnU4ChDoAQgbMAA#v=onepage&q=bin%2Fplugin%20-install%20elasticsearch%2Felasticsearch-mapper-attachments&f=false
 */
public class JestPocAttachmentTest {

    public static final String JEST_POC_ATTACH_INDEX = "jest_poc_attach_index";
    public static final String ATTACH_TYPE = "attachment_item";

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
                new IndicesExists.Builder(JEST_POC_ATTACH_INDEX).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(JEST_POC_ATTACH_INDEX).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + JEST_POC_ATTACH_INDEX + " is not deleted");
            }
        }
    }

    @Test
    public void test() throws Exception{

        //create the index
        //https://github.com/elastic/elasticsearch-mapper-attachments/issues/16
        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .put("index.mapping.attachment.detect_language", true) //expensive
                .build();
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(JEST_POC_ATTACH_INDEX)
                .settings(indexSettings)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        //create the mapping, see https://github.com/elastic/elasticsearch-mapper-attachments
        PutMapping putMapping = new PutMapping.Builder(
                JEST_POC_ATTACH_INDEX,
                ATTACH_TYPE,
                "{"
                    + "\"" + ATTACH_TYPE + "\" : {"
                        + "\"properties\" : {"
                            + "\"the_file\" : {"
                                + "\"type\" : \"attachment\","
                                + "\"fields\" : {"
                                    + "\"content\" : {"
                                        + "\"term_vector\" : \"with_positions_offsets\", " //for highlighting
                                        + "\"store\" : \"yes\" },"
                                    + "\"title\" : { \"store\" : \"yes\" },"
                                    + "\"date\" : { \"store\" : \"yes\" },"
                                    + "\"author\" : { \"store\" : \"yes\" },"
                                    + "\"content_type\" : { \"store\" : \"yes\" },"
                                    + "\"content_length\" : { \"store\" : \"yes\" },"
                                    + "\"language\" : { \"store\" : \"yes\" }"
                                + "}"
                            + "}"
                        + "}"
                    + "}"
                + "}"
            ).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        //read in a pdf, base64-encode it and index it. also refresh!
        byte[] base64Bytes = Base64.encodeBase64(IOUtils.toByteArray(
                getClass().getResourceAsStream("/lorem-ipsum.pdf"))
        );

        String body = XContentFactory.jsonBuilder()
                .startObject()
                .field("the_file", new String(base64Bytes)) //must be a string !
                .endObject().string();
        DocumentResult indexResult = client.execute(
                new Index.Builder(body)
                        .index(JEST_POC_ATTACH_INDEX)
                        .type(ATTACH_TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build()
        );
        Assert.assertNotNull(indexResult);
        Assert.assertTrue(indexResult.isSucceeded());

        //get it again
        DocumentResult getResult = client.execute(
                new Get.Builder(JEST_POC_ATTACH_INDEX, indexResult.getId())
                        .type(ATTACH_TYPE)
                        .build()
        );
        Assert.assertNotNull(getResult);
        Assert.assertTrue(getResult.isSucceeded());

        //search for "documents" and expect 1 result
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("the_file.content", "proin"))
                .highlight(new HighlightBuilder().field("the_file.content"))
        ;

        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                .addIndex(JEST_POC_ATTACH_INDEX)
                .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }
}
