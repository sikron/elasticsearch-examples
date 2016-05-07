package com.skronawi.elasticsearch.examples.scenario.basic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Refresh;
import io.searchbox.indices.mapping.DeleteMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class Admin {

    private final JestClient client;

    public Admin(String url, int timeOutSec) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(url).multiThreaded(true)
                .readTimeout(timeOutSec * 1000).build());
        client = factory.getObject();
    }

    public void shutdown() {
        client.shutdownClient();
    }

    public void refresh(String indexName) throws IOException {
        client.execute(new Refresh.Builder().addIndex(indexName).build());
    }

    public JestResult createIndex(String name) throws IOException {

        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .put("index.mapping.attachment.detect_language", true) //expensive
                .build();
        return client.execute(new CreateIndex.Builder(name).settings(indexSettings).build());
    }

    public JestResult deleteIndex(String name) throws IOException {
        return client.execute(new DeleteIndex.Builder(name).build());
    }

    public JestResult createMapping(String indexName, String mappingName, String mapping) throws IOException {
        return client.execute(new PutMapping.Builder(indexName, mappingName, mapping).build());
    }

    public JestResult deleteMapping(String indexName, String mappingName) throws IOException {
        return client.execute(new DeleteMapping.Builder(indexName, mappingName).build());
    }

    public JestResult getIndices(){
        throw new UnsupportedOperationException("not yet implemented");
    }

    public JestResult getMappings(String indexName){
        throw new UnsupportedOperationException("not yet implemented");
    }
}
