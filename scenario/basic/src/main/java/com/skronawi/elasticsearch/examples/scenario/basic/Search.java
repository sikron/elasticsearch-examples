package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.SearchResult;

import java.io.IOException;

public class Search {

    private final JestClient client;

    public Search(String url, int timeOutSec) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(url).multiThreaded(true)
                .readTimeout(timeOutSec * 1000).build());
        client = factory.getObject();
    }

    public void shutdown() {
        client.shutdownClient();
    }

    public SearchResult search(String query, String indexName) throws IOException {
        return client.execute(new io.searchbox.core.Search.Builder(query).addIndex(indexName).build());
    }
}
