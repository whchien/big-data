package nl.uva.bigdata.hadoop.assignment2;

import nl.uva.bigdata.hadoop.HadoopJob;
import nl.uva.bigdata.hadoop.exercise2.DenseVector;
import nl.uva.bigdata.hadoop.exercise2.Vector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import static java.lang.Double.*;

public class SparseMatrixVectorMultiplication extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path matrix = new Path(parsedArgs.get("--matrix"));
        Path vector = new Path(parsedArgs.get("--vector"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job multiplication = prepareJob(onCluster, jobConf,
                matrix, outputPath, TextInputFormat.class, RowDotProductMapper.class,
                Text.class, NullWritable.class, TextOutputFormat.class);

        multiplication.addCacheFile(vector.toUri());
        multiplication.waitForCompletion(true);

        return 0;
    }

    static class RowDotProductMapper extends Mapper<Object, Text, Text, NullWritable> {

        Vector vector;

        @Override
        protected void setup(Context context) throws IOException {
            URI vectorFile = context.getCacheFiles()[0];
            FileSystem fileSystem = FileSystem.get(vectorFile, context.getConfiguration());

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(vectorFile))))) {
                String[] tokens = reader.readLine().split(";");
                int len = tokens.length;
                double[] values = new double[len];

                for (int i = 0; i < len; i++) {
                    values[i] = Double.parseDouble(tokens[i]);
                }

                if (isSparseVector(values)) {
                    vector = new SparseVector(values);
                } else {
                    vector = new DenseVector();
                    ((DenseVector) vector).set(values);
                }
            }
        }

        private static final Text output = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String[] strValues = tokens[1].split(";");
            int len = strValues.length;
            double[] doubleValues = new double[len];


            for (int i = 0; i < len; i++) {
                doubleValues[i] = Double.parseDouble(strValues[i]);
            }

            Vector other;

            if (isSparseVector(doubleValues)) {
                other = new SparseVector(doubleValues);
            } else {
                other = new DenseVector();
                ((DenseVector) other).set(doubleValues);
            }

            output.set(tokens[0] + "\t" + other.dot(this.vector));
            context.write(output, NullWritable.get());
        }

        private boolean isSparseVector(double[] doubleValues) {

            long count = 0;
            for (double value : doubleValues) {
                if (value == 0) {
                    count += 1;
                }
            }
            Boolean result = 0.5 * doubleValues.length <= count;
            return result;
        }
    }
}
