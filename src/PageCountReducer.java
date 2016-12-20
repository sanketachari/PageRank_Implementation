import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sanket on 11/14/16.
 */
public class PageCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int result = 0;
        for (IntWritable value: values)
            result += value.get();

        context.write(new Text(""+ result), null);
    }
}
