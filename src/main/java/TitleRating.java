import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TitleRating {

    private static Map<String, Double> ratings = new HashMap<String, Double>();

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] mapsideFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(mapsideFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = reader.readLine(); // ignorar primeira linha
            while((line = reader.readLine()) != null){
                String[] words = line.split("\t");
                ratings.put(words[0], Double.parseDouble(words[1]));
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            Double rating = ratings.get(words[0]);
            if(rating != null){
                context.write(new Text(words[2]), new Text(rating.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "wordcount");

        job.setJarByClass(TitleRating.class);
        job.setMapperClass(RatingMapper.class);

        job.setNumReduceTasks(0);
        job.addCacheFile(URI.create("title.ratings.tsv"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("title.basics.tsv.bz2"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("result_rating"));

        long begin, end, dt;
        begin = System.currentTimeMillis();
        job.waitForCompletion(true);
        end = System.currentTimeMillis();
        dt = end - begin;
        System.out.println("Time to compute: " + dt + " milliseconds.");
    }
}
