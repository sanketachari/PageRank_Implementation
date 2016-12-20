import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sanket on 11/14/16.
 */
public class AdjacencyGraphMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        Text word = new Text();

        String line = value.toString();
        String[] parts = line.split("\\t");

        if (parts.length <= 0)
            return;

        if (parts.length > 1)
            word.set(parts[1]);
        else
            word.set("");
        context.write(new Text(parts[0]), word);
    }
}
