package com.wang.springkettle.kettle;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyJobTest {
    private MyJob myJob = new MyJob();
    private String[] params = {};
    private Map<String,String> map = new HashMap<>();
    private List<String> mesg = new ArrayList<>();

    @Test
    public void runJob() {
        String jobPath ="C:/Users/wang/Desktop/模块练习资料/importData.kjb";

        map.put("filepath","C:/Users/wang/Desktop/模块练习资料/");
        map.put("filename","适用微信公众号、APP支付报表.csv");
        map.put("hostname","127.0.0.1");
        map.put("dbname","kettle_exercise1");
        map.put("port","3306");
        map.put("username","root");
        map.put("password","root");

        mesg.add("myFileStatusCheck");  //文件状态
        mesg.add("myFileTypeCheck");    //文件后缀
        mesg.add("dataIntegrity");      //文件完整性
        mesg.add("myFileFieldCheck");   //字段信息


        myJob.runJob(params,jobPath,map,mesg);

    }
    @Test
    public void t(){

    }
}