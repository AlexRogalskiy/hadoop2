package com.c4soft.hadoop.store;

/**
 * @author Ch4mp
 * 
 */
public class Customer extends Entity {

    private static final long serialVersionUID = -1953880021581401417L;

    private String firstName;
    private String lastName;
    private String postCode;

    public Customer() {
        this(null, null, null, null);
    }

    public Customer(String firstName, String lastName, String postCode) {
        this(null, firstName, lastName, postCode);
    }

    public Customer(Long id, String firstName, String lastName, String postCode) {
        super(id);
        this.firstName = firstName;
        this.lastName = lastName;
        this.postCode = postCode;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getPostCode() {
        return postCode;
    }

    public void setPostCode(String postCode) {
        this.postCode = postCode;
    }

}
