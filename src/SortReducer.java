import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sanket on 11/15/16.
 */
public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        for (Text value: values){
            context.write(value, key);
        }
    }
}
