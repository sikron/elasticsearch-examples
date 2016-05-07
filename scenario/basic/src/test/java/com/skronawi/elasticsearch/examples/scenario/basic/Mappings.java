package com.skronawi.elasticsearch.examples.scenario.basic;

import java.io.IOException;

public class Mappings {

    public static String entry() throws IOException {
        return FileHelper.resourceAsString("/mappings/entry.json");
    }
}
