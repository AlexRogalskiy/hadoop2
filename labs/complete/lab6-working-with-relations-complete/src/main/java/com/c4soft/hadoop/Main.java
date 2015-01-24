package com.c4soft.hadoop;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {

    protected Main() {
    }

    public static void main(String[] arguments) {
        new ClassPathXmlApplicationContext("job-context.xml").close();
    }

}
