package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.DeleteByQuery;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;

import java.io.IOException;

public class Index {

    private final JestClient client;

    public Index(String url, int timeOutSec) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(url).multiThreaded(true)
                .readTimeout(timeOutSec * 1000).build());
        client = factory.getObject();
    }

    public void shutdown() {
        client.shutdownClient();
    }

    public DocumentResult create(String entry, String indexName, String mappingName) throws IOException {
        return client.execute(new io.searchbox.core.Index.Builder(entry).index(indexName)
                .type(mappingName).build());
    }

    public DocumentResult get(String id, String indexName, String mappingName) throws IOException {
        return client.execute(new Get.Builder(indexName, id).type(mappingName).build());
    }

    public JestResult delete(String id, String indexName, String mappingName) throws IOException {
        return client.execute(new Delete.Builder(id).index(indexName).type(mappingName).build());
    }

    public JestResult deleteByQuery(String query, String indexName, String mappingName) throws IOException {
        return client.execute(new DeleteByQuery.Builder(query).addIndex(indexName).addType(mappingName).build());
    }
}
