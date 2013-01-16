package com.mirantis.swift.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 */
public class TestJob extends Configured implements Tool {
  public static class TestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final InetAddress LOCAL;

    static {
      try {
        LOCAL = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw new IllegalStateException(e);
      }
    }

    private static final Log LOGGER = LogFactory.getLog(TestMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      LOGGER.info(String.format("[%s/%s] Processing value: %s", LOCAL.getHostName(), LOCAL.getHostAddress(), value));
      context.write(value, new LongWritable(1l));
    }
  }

  public static class TestReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      long summ = 0;
      for (LongWritable value : values) {
        summ += value.get();
      }

      context.write(key, new LongWritable(summ));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    final Configuration conf = getConf();
    conf.set("mapred.min.split.size", String.valueOf(32 * 1024 * 1024));
    conf.set("mapred.max.split.size", String.valueOf(64 * 1024 * 1024));

    if (args.length == 0)
      throw new IllegalArgumentException("Please specify all params: -host swift-host -tenant swift-tenant" +
              "-username login -pass password -input input-path -output output-path");

    //Swift auth properties
    conf.set("swift.auth.url", getParam(args, "host"));
    conf.set("swift.tenant", getParam(args, "tenant"));
    conf.set("swift.username", getParam(args, "username"));
    conf.set("swift.password", getParam(args, "pass"));
    conf.setInt("swift.http.port", 8080);
    conf.setInt("swift.https.port", 443);

    final Job job = new Job(conf, "Test Job");
    job.setJarByClass(TestJob.class);

    FileInputFormat.setInputPaths(job, getParam(args, "input"));
    FileOutputFormat.setOutputPath(job, new Path(getParam(args, "output")));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(TestMapper.class);
    job.setReducerClass(TestReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  private String getParam(String[] source, String key) {
    key = "-".concat(key);
    for (int i = 0; i < source.length; i++) {
      if (source[i].equals(key))
        return source[i + 1];
    }

    throw new IllegalArgumentException("Specified key: " + key + " not found");
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new TestJob(), args);
  }
}
