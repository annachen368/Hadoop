import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanScore {

  public static class MeanMap extends Mapper<Object, Text, Text, IntWritable>{

    private Text name = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      System.out.println(line);
      StringTokenizer itr = new StringTokenizer(line, "\n");
      while (itr.hasMoreTokens()) {
        StringTokenizer tokenizerLine = new StringTokenizer(itr.nextToken());
        String strName = tokenizerLine.nextToken();
        String strScore = tokenizerLine.nextToken();
        name.set(strName);
        int scoreInt = Integer.parseInt(strScore);
        context.write(name, new IntWritable(scoreInt));
      }
    }
  }

  public static class MeanReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      int count = 0;
      for(IntWritable val : values) {
        sum += val.get();
        count++;
      }
	System.out.println("mycount="+count);
      result.set((int)sum/count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Mean Score");
    job.setJarByClass(MeanScore.class);
    job.setMapperClass(MeanMap.class);
    //job.setCombinerClass(MeanReduce.class);
    job.setReducerClass(MeanReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
