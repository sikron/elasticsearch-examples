package com.skronawi.elasticsearch.examples.jest.poc;

import java.util.*;

public class SimpleItem {

    private String name;
    private long amount;
    private List<String> tags;
    private Date date;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public static SimpleItem dummy() {
        SimpleItem simpleItem = new SimpleItem();
        Random random = new Random();
        simpleItem.setAmount(random.nextLong());
        simpleItem.setName("name" + UUID.randomUUID().toString());
        simpleItem.setTags(Arrays.asList("a", "$", "1", ":", ";", "[]"));
        simpleItem.setDate(Calendar.getInstance().getTime());
        return simpleItem;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleItem simpleItem = (SimpleItem) o;

        if (amount != simpleItem.amount) return false;
        if (name != null ? !name.equals(simpleItem.name) : simpleItem.name != null) return false;
        if (tags != null ? !tags.equals(simpleItem.tags) : simpleItem.tags != null) return false;
        return date != null ? date.equals(simpleItem.date) : simpleItem.date == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (amount ^ (amount >>> 32));
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }
}
