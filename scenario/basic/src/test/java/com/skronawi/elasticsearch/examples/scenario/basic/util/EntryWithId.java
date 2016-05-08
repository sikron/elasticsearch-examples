package com.skronawi.elasticsearch.examples.scenario.basic.util;

import com.skronawi.elasticsearch.examples.scenario.basic.Entry;
import io.searchbox.annotations.JestId;

//with Jest the id can be retrieved only by using @JestId, see
//http://stackoverflow.com/questions/33352798/elasticsearch-jest-client-how-to-return-document-id-from-hit/33662542#33662542
public class EntryWithId extends Entry {

    @JestId
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
