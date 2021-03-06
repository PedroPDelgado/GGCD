import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Genre_FilmCountWithCombiner {
    public static class Genre_FimCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            String[] genres = words[words.length - 1].split(",");
            for (String genre: genres){
                context.write(new Text(genre.toLowerCase()), new LongWritable(1));
            }
        }
    }

    public static class Genre_FilmCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value: values){
                total += value.get();
            }
            context.write(key, new LongWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "wordcount");

        job.setJarByClass(Genre_FilmCountWithCombiner.class);
        job.setMapperClass(Genre_FimCountMapper.class);
        job.setCombinerClass(Genre_FilmCountReducer.class);
        job.setReducerClass(Genre_FilmCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("title.basics.tsv.bz2"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("result_combiner"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        long begin, end, dt;
        begin = System.currentTimeMillis();
        job.waitForCompletion(true);
        end = System.currentTimeMillis();
        dt = end - begin;
        System.out.println("Time to compute: " + dt + " milliseconds.");
    }
}
