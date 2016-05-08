package com.skronawi.elasticsearch.examples.scenario.basic;

import com.google.gson.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.SearchResult;
import org.joda.time.DateTime;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;

/*
for Joda date conversion: http://jtruty.github.io/programming/2015/04/03/elasticsearch-http-queries-with-jest.html
 */
public class Search {

    private final JestClient client;

    private class DateTimeTypeConverter implements JsonSerializer<Date>, JsonDeserializer<Date> {

        @Override
        public JsonElement serialize(Date src, Type srcType, JsonSerializationContext context) {
            return new JsonPrimitive(src.toString());
        }

        @Override
        public Date deserialize(JsonElement json, Type type, JsonDeserializationContext context)
                throws JsonParseException {
            return new DateTime(json.getAsLong()).toDate();
        }
    }

    public Search(String url, int timeOutSec) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Date.class, new DateTimeTypeConverter()).create();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(url).multiThreaded(true)
                .readTimeout(timeOutSec * 1000).gson(gson).build());
        client = factory.getObject();
    }

    public void shutdown() {
        client.shutdownClient();
    }

    public SearchResult search(String query, String indexName) throws IOException {
        return client.execute(new io.searchbox.core.Search.Builder(query).addIndex(indexName).build());
    }
}
