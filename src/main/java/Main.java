import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class Main{
    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        //System.setProperty("hadoop.home.dir", "D:\\hadoop");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aeroports");
        job.setJarByClass(Main.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/traffic1hour.exp2"));
        FileOutputFormat.setOutputPath(job, new Path("results"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("1");
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

//    public int run(String[] args) throws Exception {
//        Job job = Job.getInstance(getConf(), "aeroports");
//        job.setJarByClass(this.getClass());
//        FileInputFormat.addInputPath(job, new Path("traffic1hour.exp2"));
//        FileOutputFormat.setOutputPath(job, new Path("results"));
//        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        return job.waitForCompletion(true) ? 0 : 1;
//    }

    /* When you read a file with a M/R program, the input key of your mapper should be the index of the line in the file, while the input value will be the full line. */
/* In input2.txt, value is a line with the format "city,state,temperature" (ex: Ujjain,MP,77) */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //System.out.println(key.toString());
            //System.out.println(value.toString());
            String[] subvalues = value.toString().split(";");
            Text key1 = new Text(subvalues[0]);
            Text key2 = new Text(subvalues[1]);
            IntWritable value1 = new IntWritable(1);
            context.write(key1, value1);
            context.write(key2, value1);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumOfAirports = 0;
            for (IntWritable airport : values) {
                sumOfAirports++;
            }
            context.write(key, new IntWritable(sumOfAirports));

        }
    }
}
