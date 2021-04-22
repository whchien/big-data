package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.regex.Pattern;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);
    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));


    // TODO Implement me

    Job job = prepareJob(onCluster, jobConf, books, outputPath,
            TextInputFormat.class, BookYearMapper.class, IntWritable.class, Text.class,
            ReduceSideJoinReducer.class, Text.class, NullWritable.class, TextOutputFormat.class);

    job.addCacheFile(authors.toUri());
    job.waitForCompletion(true);

    return 0;

  }

  public static class BookYearMapper extends Mapper <Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String record = value.toString();
      String[] splits = record.split("\t");

      int joinKey = Integer.parseInt(splits[0]);
      String year = splits[1];
      String title = splits[2];

      IntWritable authorID = new IntWritable(joinKey);
      Text yearTitle = new Text(title + "\t" + year);

      context.write(authorID, yearTitle);
    }
  }

  public static class ReduceSideJoinReducer extends Reducer <IntWritable, Text, Text, NullWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("\t");
    private static final Map<Integer, String> AUTHORS = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      URI authorsFile = context.getCacheFiles()[0];
      FileSystem fs = FileSystem.get(authorsFile, context.getConfiguration());

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(authorsFile))))) {
        String line = reader.readLine();
        while (line != null) {
          String[] tokens = SEPARATOR.split(line);
          Integer authorId = Integer.parseInt(tokens[0]);
          String authorName = tokens[1];
          AUTHORS.put(authorId, authorName);

          line = reader.readLine();
        }
      }
    }

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      String author = AUTHORS.get(key.get());
      for (Text titleYear: values){
        String authorTitleYear = author + "\t" + titleYear.toString();
        Text output = new Text(authorTitleYear);
        context.write(output, NullWritable.get());
      }
    }
  }

}