package com.wang.springkettle.kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.util.List;
import java.util.Map;

public class MyJob {

    /**
     * @param params  传递参数
     * @param jobPath job文件路径
     * @param map     命名参数
     * @param varName    运行结果变量名
     */
    public void runJob(String[] params, String jobPath, Map<String, String> map, List<String> varName) {
        try {
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            //
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);

            //设置命名参数
            for (String key : map.keySet()) {
                jobMeta.setParameterValue(key, map.get(key));
            }
            jobMeta.setInternalKettleVariables(job);
            jobMeta.activateParameters();

            job.start();

            job.waitUntilFinished();

            //获得转换运行结果变量
            for (String vn : varName) {
                String message = job.getVariable(vn);
                System.out.println(vn + " : " + ("true".equals(message) ? "success" : "failure"));
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
