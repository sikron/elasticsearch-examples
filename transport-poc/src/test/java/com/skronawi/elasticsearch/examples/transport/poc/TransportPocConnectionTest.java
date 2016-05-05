package com.skronawi.elasticsearch.examples.transport.poc;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;

/*
https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html
http://www.programcreek.com/java-api-examples/index.php?api=org.elasticsearch.client.transport.TransportClient

https://www.elastic.co/blog/found-java-clients-for-elasticsearch
 */
public class TransportPocConnectionTest {

    private TransportClient client;

    @BeforeClass
    public void setupClient() throws Exception {
        client = TransportClient.builder().build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getLocalHost(), 9300));
    }

    @AfterClass
    public void teardownClient() throws Exception {
        if (client == null){
            return;
        }
        client.close();
    }

    @Test
    public void testConnectedNodes() throws Exception {
        client.connectedNodes()
                .stream()
                .forEach(node -> System.out.println(node.getId() + ", " + node.getName()));
    }

    @Test
    public void testFilteredNodes() throws Exception {
        client.filteredNodes()
                .stream()
                .forEach(node -> System.out.println(node.getId() + ", " + node.getName()));
    }

    @Test
    public void testListedNodes() throws Exception {
        client.listedNodes()
                .stream()
                .forEach(node -> System.out.println(node.getId() + ", " + node.getName()));
    }
}
