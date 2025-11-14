import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collections;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrossCorrelationStripes {

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, MapWritable> {

        private Text pair = new Text();

        public void map(Object placeholder, Text value, Context context)
            throws IOException, InterruptedException {
            var tmp = value.toString();
            var entries = Arrays.asList(tmp.split("\\|"));
            if (entries.size() < 3) {
                return;
            }
            entries = entries.subList(1, entries.size());
            Collections.sort(entries);
            var occs = new HashSet();
            for (int i = 0; i < entries.size(); i++) {
                var first = entries.get(i);
                if (occs.contains(first)) {
                    continue;
                }
                occs.add(first);

                var acc = new MapWritable();
                var item = new Text(first);

                for (int j = i + 1; j < entries.size(); j++) {
                    var second = entries.get(j);
                    var diff = first.compareTo(second);

                    if (diff == 0)
                        continue;

                    var key = new Text(second);

                    if (acc.containsKey(key)) {
                        IntWritable val = (IntWritable)acc.get(key);
                        val.set(val.get() + 1);
                        acc.put(key, val);
                    } else {
                        acc.put(key, new IntWritable(1));
                    }
                }

                context.write(item, acc);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text, MapWritable, Text, IntWritable> {

        public void reduce(Text bigKey, Iterable<MapWritable> values,
                           Context context)
            throws IOException, InterruptedException {

            var acc = new MapWritable();

            for (MapWritable map : values) {
                for (Map.Entry entry : map.entrySet()) {
                    var key = (Text)entry.getKey();
                    var val = (IntWritable)entry.getValue();

                    if (acc.containsKey(key)) {
                        IntWritable oldVal = (IntWritable)acc.get(key);
                        oldVal.set(oldVal.get() + val.get());
                        acc.put(key, oldVal);
                    } else {
                        acc.put(key, val);
                    }
                }
            }

            for (Map.Entry entry : acc.entrySet()) {
                var key = (Text)entry.getKey();
                var value = (IntWritable)entry.getValue();

                context.write(
                    new Text(bigKey.toString() + " " + key.toString()), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cross-Corelation Stripes");
        job.setJarByClass(CrossCorrelationStripes.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
