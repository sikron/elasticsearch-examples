package com.skronawi.elasticsearch.examples.scenario.basic;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class FileHelper {

    public static String resourceAsBase64String(String path) throws IOException {
        return Base64.encodeBase64String(IOUtils.toByteArray(FileHelper.class.getResourceAsStream(path)));
    }

    public static String resourceAsString(String path) throws IOException {
        return new String(IOUtils.toByteArray(FileHelper.class.getResourceAsStream(path)));
    }
}
