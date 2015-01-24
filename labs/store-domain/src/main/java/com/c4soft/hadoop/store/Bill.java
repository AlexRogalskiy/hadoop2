package com.c4soft.hadoop.store;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ch4mp
 * 
 */
public class Bill extends Entity {

    private static final long serialVersionUID = -8915906113053368953L;

    private Date timeStamp;
    private Customer customer;
    private Map<Product, Double> products;

    public Bill() {
        this(null, null, null, null);
    }

    public Bill(Date timeStamp, Customer customer, Map<Product, Double> products) {
        this(null, timeStamp, customer, products);
    }

    public Bill(Long id, Date timeStamp, Customer customer, Map<Product, Double> products) {
        super(id);
        this.timeStamp = timeStamp;
        this.customer = customer;
        if (products == null) {
            this.products = new HashMap<Product, Double>();
        } else {
            this.products = products;
        }
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Map<Product, Double> getProducts() {
        return products;
    }

    public void setProducts(Map<Product, Double> products) {
        this.products = products;
    }

}
