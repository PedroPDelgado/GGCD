import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SelectorSorter {
    public static class SelectorMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            Double rating = Double.parseDouble(words[1]);
            if(rating >= 9.0){
                context.write(new DoubleWritable(rating), new Text(words[0]));
            }
        }
    }

    public static class DescendingKeyComparator extends DoubleWritable.Comparator {

        // NOTA: multiplica-se o resultado do compare por -1 para os resultados serem decrescentes
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -1 * super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "selectorSorter");

        job.setJarByClass(SelectorSorter.class);
        job.setMapperClass(SelectorMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("result_rating/part-m-00000"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("result_selected"));
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(DescendingKeyComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
