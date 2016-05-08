package com.skronawi.elasticsearch.examples.scenario.basic.util;

import com.skronawi.elasticsearch.examples.scenario.basic.FileHelper;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class Contents {

    public static String resourceAsString(String path) throws IOException {
        return new String(IOUtils.toByteArray(FileHelper.class.getResourceAsStream(path)));
    }

    public static String asBase64(String content){
        return Base64.encodeBase64String(content.getBytes());
    }
}
