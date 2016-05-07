package com.skronawi.elasticsearch.examples.scenario.basic;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class BasicScenarioBase {

    protected static final String URL = "http://localhost:9200";
    protected static final String INDEX_NAME = "scenario";
    protected static final String MAPPING_NAME = "entry";

    protected Admin admin;
    protected Index index;
    protected Search search;

    @BeforeClass
    public void setup() throws Exception {

        admin = new Admin(URL, 5);
        admin.createIndex(INDEX_NAME);

        index = new Index(URL, 30);
        search = new Search(URL, 10);
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        if (admin != null) {
            admin.deleteIndex(INDEX_NAME);
            admin.shutdown();
        }
        if (index != null) {
            index.shutdown();
        }
        if (search != null) {
            search.shutdown();
        }
    }
}
