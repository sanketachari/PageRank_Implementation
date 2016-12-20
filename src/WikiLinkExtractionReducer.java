import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by sanket on 11/14/16.
 */
public class WikiLinkExtractionReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        boolean isRedLink = true;
        HashSet<String> links = new HashSet<>();

        for (Text value: values){
            if (value.toString().equals("*")){
                isRedLink = false;
            }
            links.add(value.toString());
        }

        if (!isRedLink){
            for (String link: links){
                if (!link.toString().equals("*"))
                    context.write(new Text(link), key);
                else
                    context.write(null, key);
            }
        }
    }
}
