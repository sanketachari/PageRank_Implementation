import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by sanket on 11/15/16.
 */
public class SortMapper extends Mapper<Text, Text, DoubleWritable, Text> {

    private static double rankThreshold;

    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int nodes = Integer.parseInt(conf.get("numOfNodes"));
        rankThreshold = (double) 5/nodes;
    }

    public void map (Text key, Text value, Context context ) throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\t");
        Double pageRank = Double.parseDouble(st.nextToken());

        if (pageRank < rankThreshold)
            return;

        context.write(new DoubleWritable(pageRank), key);
    }
}
