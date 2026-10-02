package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class HadoopSmokeTest {

    @Test
    void hadoopClientIsOnClasspath() throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "smoke");

        assertThat(job.getJobName()).isEqualTo("smoke");
        assertThat(Runtime.version().feature()).isEqualTo(17);
    }
}
