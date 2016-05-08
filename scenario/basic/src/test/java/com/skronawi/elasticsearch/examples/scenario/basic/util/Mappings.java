package com.skronawi.elasticsearch.examples.scenario.basic.util;

import java.io.IOException;

public class Mappings {

    public static String entry() throws IOException {
        return Contents.resourceAsString("/mappings/entry.json");
    }
}
