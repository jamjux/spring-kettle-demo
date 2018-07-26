package com.wang.springkettle.kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class MyTrans {
    private String path = "";

    /**
     * 调用transformation
     *
     * @param params
     * @param ktrPath
     * @throws Exception
     */
    public void runTrans(String[] params, String ktrPath) {
        Trans trans = null;
        try {
            //初始化
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            //转换元对象
            TransMeta transMeta = new TransMeta(ktrPath);
            //创建转换
            trans = new Trans(transMeta);
            //执行转换
            trans.setVariable("max","99");
            trans.setParameterValue("no", "11");
            trans.execute(params);
            //等待执行结束
            trans.waitUntilFinished();
            //异常
            if (trans.getErrors() > 0) {
                throw new Exception("There are errors during transformation exception!(传输过程中发生异常)");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param params  传递参数
     * @param jobPath job文件路径
     */
    public void runJob(String[] params, String jobPath) {
        try {
            KettleEnvironment.init();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            // job.setVariable(paraname, paravalue);
            //job.setVariable("no", "13");
            job.setVariable("max", "100");
            job.setParameterValue("no", "20");
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
