package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HadoopSmokeTest {

    @Test
    void hadoopClientIsOnClasspath() throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "smoke");

        assertEquals("smoke", job.getJobName());
        assertEquals(17, Runtime.version().feature());
    }
}
