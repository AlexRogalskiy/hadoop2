package com.c4soft.hadoop.store;

/**
 * @author Ch4mp
 * 
 */
public abstract class LabeledEntity extends Entity {

    private static final long serialVersionUID = 3118753870458998230L;

    private String label;

    public LabeledEntity() {
        this(null, null);
    }

    public LabeledEntity(String label) {
        this(null, label);
    }

    public LabeledEntity(Long id, String label) {
        super(id);
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

}
