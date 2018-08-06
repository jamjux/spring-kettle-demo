package com.wang.springkettle.kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyJob {
    /**
     * @param params  传递参数
     * @param jobPath job文件路径
     */
    public void runJob(String[] params, String jobPath, Map<String,String> map ,List<String> mesg) {
        try {
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);

            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            // job.setVariable(paraname, paravalue);
            //job.setVariable("no", "13");
            //job.setVariable("max", "100");
            //添加命名参数定义
            //job.addParameterDefinition("no", "13", "excel记录数");
            //job.addParameterDefinition("xu", "13", "测试用");
            //job.setParameterValue("no", "11");
            //job.setParameterValue("xu", "11");
            for (String key: map.keySet()) {
                jobMeta.setParameterValue(key,map.get(key));
            }
            jobMeta.setInternalKettleVariables(job);
            jobMeta.activateParameters();

            job.start();

            job.waitUntilFinished();

            for (String m:mesg) {
                String message = job.getVariable(m);
                System.out.println(m+" : "+ (message != null ? "success" : "failure"));
            }

            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
