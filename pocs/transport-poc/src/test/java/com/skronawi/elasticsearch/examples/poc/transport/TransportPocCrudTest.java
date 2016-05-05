package com.skronawi.elasticsearch.examples.poc.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skronawi.elasticsearch.examples.poc.common.SimpleItem;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;

/*
https://www.elastic.co/blog/found-java-clients-for-elasticsearch
 */
public class TransportPocCrudTest {

    public static final String TRANSPORT_POC_INDEX = "transport_poc_index";
    public static final String SIMPLE_ITEM_TYPE = "simple_item";

    private TransportClient client;
    private ObjectMapper objectMapper;

    @BeforeClass
    public void setupClient() throws Exception {
        client = TransportClient.builder().build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        objectMapper = new ObjectMapper();
    }

    @AfterClass(alwaysRun = true)
    public void teardownClient() throws Exception {
        if (client == null) {
            return;
        }
        client.close();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {

        if (client == null) {
            return;
        }

        boolean indexExists = client.admin().indices()
                .prepareExists(TRANSPORT_POC_INDEX).execute().actionGet().isExists();
        if (indexExists) {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices()
                    .prepareDelete(TRANSPORT_POC_INDEX).execute().actionGet();
            if (!deleteIndexResponse.isAcknowledged()) {
                System.out.println("index " + TRANSPORT_POC_INDEX + " not deleted");
            }
        }
    }

    @Test
    public void createGet() throws Exception {

        SimpleItem dummy = SimpleItem.dummy();
        String dummyAsString = objectMapper.writeValueAsString(dummy);

        IndexResponse indexResponse = client.prepareIndex()
                .setIndex(TRANSPORT_POC_INDEX)
                .setType(SIMPLE_ITEM_TYPE)
                .setSource(dummyAsString)
                .execute().actionGet();

        Assert.assertEquals(indexResponse.getIndex(), TRANSPORT_POC_INDEX);
        Assert.assertEquals(indexResponse.getType(), SIMPLE_ITEM_TYPE);
        Assert.assertNotNull(indexResponse.getId());
        Assert.assertTrue(indexResponse.getVersion() >= 1);

        GetResponse getResponse = client.prepareGet()
                .setIndex(TRANSPORT_POC_INDEX)
                .setId(indexResponse.getId())
                .execute().actionGet();

        String sourceAsString = getResponse.getSourceAsString();
        Assert.assertNotNull(sourceAsString);
        SimpleItem retrievedDummy = objectMapper.readValue(sourceAsString, SimpleItem.class);
        Assert.assertEquals(retrievedDummy, dummy);
    }
}
