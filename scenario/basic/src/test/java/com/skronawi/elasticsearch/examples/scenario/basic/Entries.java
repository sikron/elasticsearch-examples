package com.skronawi.elasticsearch.examples.scenario.basic;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

public class Entries {

    public static Entry legal5() throws IOException {
        Entry entry = new Entry();
        entry.setContent(FileHelper.resourceAsBase64String("/entries/letterlegal5.doc"));
        entry.setIsbn("1234");
        entry.setNumberOfPages(3);
        entry.setTitle("letterlegal5.doc");
        entry.setReleaseDate(Date.from(LocalDate.now().minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        return entry;
    }

    public static Entry lorem() throws IOException {
        Entry entry = new Entry();
        entry.setContent(FileHelper.resourceAsBase64String("/entries/lorem-ipsum.pdf"));
        entry.setIsbn("9876");
        entry.setNumberOfPages(8);
        entry.setTitle("lorem-ipsum.pdf");
        entry.setReleaseDate(Date.from(LocalDate.now().minusYears(6).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        return entry;
    }

    public static Entry test() throws IOException {
        Entry entry = new Entry();
        entry.setContent(FileHelper.resourceAsBase64String("/entries/test.pdf"));
        entry.setIsbn("5678");
        entry.setNumberOfPages(1);
        entry.setTitle("test.pdf");
        entry.setReleaseDate(Date.from(LocalDate.now().minusMonths(7).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        return entry;
    }

    public static Entry germanContract() throws IOException {
        Entry entry = new Entry();
        entry.setContent(FileHelper.resourceAsBase64String("/entries/muster-vertrag.pdf"));
        entry.setIsbn("3409");
        entry.setNumberOfPages(3);
        entry.setTitle("muster-vertrag.pdf");
        entry.setReleaseDate(Date.from(LocalDate.now().minusWeeks(2).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        return entry;
    }
}
