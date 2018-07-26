package com.wang.springkettle.kettle;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MyTransTest {

    private MyTrans myTrans = new MyTrans();
    private String[] params = {"7"};


    @Test
    public void runTrans() {
        myTrans.runTrans(params, "C:/Users/wang/Desktop/test.ktr");
    }

    @Test
    public void runJob() {
        myTrans.runJob(params, "C:/Users/wang/Desktop/test.kjb");
    }
}