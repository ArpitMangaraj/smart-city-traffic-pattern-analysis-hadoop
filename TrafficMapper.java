import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable count = new IntWritable(1);
    private Text outputKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length >= 8 && !parts[0].equals("Time")) {
            String time = parts[0];
            String situation = parts[7];
            outputKey.set("Hour:" + time.split(":")[0] + " " + situation.trim());
            context.write(outputKey, count);
        }
    }
}