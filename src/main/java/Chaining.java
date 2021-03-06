import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class Chaining {
    public static void main(String[] args) throws Exception {
        Job job1 = Job.getInstance(new Configuration(), "rater");

        job1.setJarByClass(TitleRating.class);
        job1.setMapperClass(TitleRating.RatingMapper.class);

        job1.setNumReduceTasks(0);
        job1.addCacheFile(URI.create("title.ratings.tsv"));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path("title.basics.tsv.bz2"));

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("result_rating"));


        Job job2 = Job.getInstance(new Configuration(), "selectorSorter");

        job2.setJarByClass(SelectorSorter.class);
        job2.setMapperClass(SelectorSorter.SelectorMapper.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path("result_rating/part-m-00000"));

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("result_selected"));
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setSortComparatorClass(SelectorSorter.DescendingKeyComparator.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);



        job1.waitForCompletion(true);
        job2.waitForCompletion(true);

    }
}
