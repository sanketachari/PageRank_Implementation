import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by sanket on 11/14/16.
 */
public class AdjacencyGraphReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        StringBuilder result = new StringBuilder();
        HashSet<Text> uniquePages = new HashSet<>();

        for (Text value: values){
            if ( (!value.toString().equals("*") && !value.toString().isEmpty() && uniquePages.add(value)) )
                result.append(value.toString() + "\t");
        }
        context.write(key, new Text(result.toString()));
    }
}
