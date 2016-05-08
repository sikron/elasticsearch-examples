package com.skronawi.elasticsearch.examples.scenario.basic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Date;

public class Entry {

    private String title;
    private String isbn;
    private Date releaseDate;
    private int numberOfPages;
    private String content;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public int getNumberOfPages() {
        return numberOfPages;
    }

    public void setNumberOfPages(int numberOfPages) {
        this.numberOfPages = numberOfPages;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String serialize() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
    }

    public static Entry deSerialize(String entry) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(entry, Entry.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        if (numberOfPages != entry.numberOfPages) return false;
        if (title != null ? !title.equals(entry.title) : entry.title != null) return false;
        if (isbn != null ? !isbn.equals(entry.isbn) : entry.isbn != null) return false;
        if (releaseDate != null ? !releaseDate.equals(entry.releaseDate) : entry.releaseDate != null) return false;
        return content != null ? content.equals(entry.content) : entry.content == null;

    }

    @Override
    public int hashCode() {
        int result = title != null ? title.hashCode() : 0;
        result = 31 * result + (isbn != null ? isbn.hashCode() : 0);
        result = 31 * result + (releaseDate != null ? releaseDate.hashCode() : 0);
        result = 31 * result + numberOfPages;
        result = 31 * result + (content != null ? content.hashCode() : 0);
        return result;
    }
}
