package com.skronawi.elasticsearch.examples.poc.tika;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;

public class MultiLangEntryScenario {

    public static final String INDEX = "tika_poc_index";
    public static final String TYPE = "multi-lang-entry";

    private JestClient client;

    @BeforeClass
    public void setup() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());
        client = factory.getObject();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        if (client == null) {
            return;
        }
        client.shutdownClient();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (client == null) {
            return;
        }
        JestResult indicesExistResult = client.execute(
                new IndicesExists.Builder(INDEX).build()
        );
        if (indicesExistResult.isSucceeded()) {
            JestResult deleteIndexResult = client.execute(
                    new DeleteIndex.Builder(INDEX).build()
            );
            if (!deleteIndexResult.isSucceeded()) {
                System.out.println("index " + INDEX + " is not deleted");
            }
        }
    }

    @Test
    public void test() throws Exception {

        //create the index
        Settings indexSettings = Settings.builder()
                .put("index.mapping.attachment.indexed_chars", -1) //default is "only" 100000
                .build();
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(INDEX)
                .settings(indexSettings)
                .build());
        Assert.assertTrue(createIndexResult.isSucceeded());

        PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,
                new String(IOUtils.toByteArray(getClass().getResourceAsStream("/multi-lang-entry.json")))).build();
        JestResult putMappingResult = client.execute(putMapping);
        Assert.assertTrue(putMappingResult.isSucceeded());

        ContentLocale contentLocale = extract("/muster-vertrag.pdf");

        String body = XContentFactory.jsonBuilder()
                .startObject()
                .field("title", "muster-vertrag.pdf")
                .field("text_de", contentLocale.content)
                .endObject().string();

        DocumentResult indexResult = client.execute(
                new Index.Builder(body)
                        .index(INDEX)
                        .type(TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build()
        );
        Assert.assertNotNull(indexResult);
        Assert.assertTrue(indexResult.isSucceeded());

        //get it again
        DocumentResult getResult = client.execute(
                new Get.Builder(INDEX, indexResult.getId())
                        .type(TYPE)
                        .build()
        );
        Assert.assertNotNull(getResult);
        Assert.assertTrue(getResult.isSucceeded());

        //search for "documents" and expect 1 result
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("text_de", "Auftrag"))
                //is contained, also "Aufträge", which would be analyzed also to "auftrag"
                //see    curl -XPOST 'localhost:9200/_analyze?analyzer=german&pretty' -d 'Aufträge'
                ;

        SearchResult searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);

        // now for the english document ----------------------------------------------------------------------

        contentLocale = extract("/letterlegal5.doc");

        body = XContentFactory.jsonBuilder()
                .startObject()
                .field("title", "letterlegal5.doc")
                .field("text_en", contentLocale.content)
                .endObject().string();

        indexResult = client.execute(
                new Index.Builder(body)
                        .index(INDEX)
                        .type(TYPE)
                        .setParameter(Parameters.REFRESH, true)
                        .build()
        );
        Assert.assertNotNull(indexResult);
        Assert.assertTrue(indexResult.isSucceeded());

        //get it again
        getResult = client.execute(
                new Get.Builder(INDEX, indexResult.getId())
                        .type(TYPE)
                        .build()
        );
        Assert.assertNotNull(getResult);
        Assert.assertTrue(getResult.isSucceeded());

        //search for "documents" and expect 1 result
        searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("text_en", "sampling"))
        //"sample" is contained, which would be analyzed to "sampl", just like "sampling"
        //see    curl -XPOST 'localhost:9200/_analyze?analyzer=english&pretty' -d 'sample'
        ;

        searchResult = client.execute(
                new Search.Builder(searchSourceBuilder.toString())
                        .addIndex(INDEX)
                        .build());
        Assert.assertNotNull(searchResult);
        Assert.assertEquals(searchResult.getTotal().intValue(), 1);
    }

    private ContentLocale extract(String resourcePath) throws Exception {


        Tika tika = new Tika();
        String text = null;
        try (InputStream stream = getClass().getResourceAsStream(resourcePath)) {
            text = tika.parseToString(stream);
        }

        //load all languages:
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

        //build language detector:
        LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();

        //create a text object factory
        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

        TextObject textObject = textObjectFactory.forText(text);
        Optional<LdLocale> lang = languageDetector.detect(textObject);

        Locale locale = Locale.forLanguageTag(lang.get().getLanguage());

        return new ContentLocale(text, locale);
    }

    private class ContentLocale {

        public String content;
        public Locale locale;

        public ContentLocale(String content, Locale locale) {
            this.content = content;
            this.locale = locale;
        }
    }
}
