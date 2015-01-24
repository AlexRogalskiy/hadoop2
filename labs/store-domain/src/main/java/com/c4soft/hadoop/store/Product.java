package com.c4soft.hadoop.store;

/**
 * @author Ch4mp
 * 
 */
public class Product extends LabeledEntity {

    private static final long serialVersionUID = 4149765282095009039L;

    private Double price;
    private Category category;

    public Product() {
        this(null, null, null, null);
    }

    public Product(String label, Double price, Category category) {
        this(null, label, price, category);
    }

    public Product(Long id, String label, Double price, Category category) {
        super(id, label);
        this.price = price;
        this.category = category;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Category getCategory() {
        return category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

}
