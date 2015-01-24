package com.c4soft.hadoop.store;

import java.io.Serializable;

/**
 * @author Ch4mp
 * 
 */
public abstract class Entity implements Serializable {

    private static final long serialVersionUID = -4301209630264324489L;

    private Long id;

    public Entity() {
        super();
        this.id = null;
    }

    public Entity(Long id) {
        super();
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

}
