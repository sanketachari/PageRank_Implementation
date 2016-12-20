import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by sanket on 11/15/16.
 */
public class PageRankReducer extends Reducer<Text, RankValue, Text, Text> {

    public void reduce(Text key, Iterable<RankValue> values, Context context){
        Double sum = 0.0;
        ArrayList<String> outlinks = new ArrayList<String>();

        /*
        we receive 2 different types of data associated with this key:
        1. components of the key's pagerank that we must sum together (double)
        2. outlinks from this key
        */

        for (RankValue value : values){
           // System.out.println("red"+value);

            if (!value.isOutlink()){
                sum += value.getRank().get();
            }
            else {
                Text copy = new Text(value.getOutlink());
                outlinks.add(copy.toString());
            }
        }

        String val = sum.toString();
        for (String outlink : outlinks){
            val += "\t";
            val += outlink;
        }

        try {
            context.write(key, new Text(val));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
