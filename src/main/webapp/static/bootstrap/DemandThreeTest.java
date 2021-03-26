package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * Create by RenFaXian on 2019/10/25 9:36
 */
public class DemandThreeTest {

    //map阶段
    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //重写map方法
            //切割
            String[] split = value.toString().split(",");
            //课程名
            String courseName;
            //学生名
            String studentName;
            //平均分
            Double avg = 0.0;
            //总分数
            Double sum = 0.0;
            //考试次数
            int kscount = split.length - 2;

            //求变量
            courseName = split[0];
            studentName = split[1];
            //avg = sum/kscount;
            for (int i = 2; i < split.length; i++) {
                sum += Integer.parseInt(split[i]);
            }
            avg = sum / kscount;
            context.write(new Text(courseName), new Text(studentName + "\t" + avg));
        }
    }

    //reduce阶段
    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String studentName = null;
            Double avg = 0.0;
            for (Text a : values) {
                String[] split = a.toString().split("\t");
                if (Double.parseDouble(split[1]) > avg) {
                    avg = Double.parseDouble(split[1]);
                    studentName = split[0];
                }
            }
            context.write(key, new Text(studentName + "\t" + avg));
            for (Text a : values) {
                String[] split1 = a.toString().split("\t");
                if (Double.parseDouble(split1[1]) == avg || !split1[0].equals(studentName)) {
                    avg = Double.parseDouble(split1[1]);
                    studentName = split1[0];
                    context.write(key, new Text(studentName + "\t" + avg));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration configuration = new Configuration();

        //创建JOB对象
        Job job = Job.getInstance(configuration, "renfaxian");

        //设置job处理类
        job.setJarByClass(DemandThreeTest.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Renfaxian\\Desktop\\lianxi.txt"));

        //设置map相关
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Renfaxian\\Desktop\\result2"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
