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
import org.apache.tika.Tika;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;

public class TextExtractAndLangDetect {

    //https://tika.apache.org/1.8/examples.html

    @Test
    public void testExtractFromPDF() throws Exception {
        Tika tika = new Tika();
        try (InputStream stream = getClass().getResourceAsStream("/muster-vertrag.pdf")) {
            System.out.println(tika.parseToString(stream));
        }
    }

    @Test
    public void testExtractFromDoc() throws Exception {
        Tika tika = new Tika();
        try (InputStream stream = getClass().getResourceAsStream("/letterlegal5.doc")) {
            System.out.println(tika.parseToString(stream));
        }
    }

    //https://github.com/optimaize/language-detector

    @Test
    public void detectLanguages() throws Exception {

        //load all languages:
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

        //build language detector:
        LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();

        //create a text object factory
        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

        //english
        Tika tika = new Tika();
        String text = null;
        try (InputStream stream = getClass().getResourceAsStream("/letterlegal5.doc")) {
            text = tika.parseToString(stream);
        }

        //query:
        TextObject textObject = textObjectFactory.forText(text);
        Optional<LdLocale> lang = languageDetector.detect(textObject);
        System.out.println(lang.get().getLanguage());

        //german
        text = null;
        try (InputStream stream = getClass().getResourceAsStream("/muster-vertrag.pdf")) {
            text = tika.parseToString(stream);
        }

        //query:
        textObject = textObjectFactory.forText(text);
        lang = languageDetector.detect(textObject);
        System.out.println(lang.get().getLanguage());
    }
}
