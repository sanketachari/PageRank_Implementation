import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by sanket on 11/15/16.
 */
public class PageRankMapper extends Mapper<Text, Text, Text, RankValue> {

    private static final double d = 0.85;
    private static Integer n;

    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        n = Integer.parseInt(conf.get("numOfNodes"));
    }

    public void map(Text key, Text value, Context context){
        StringTokenizer st = new StringTokenizer(value.toString(), "\t");
        int degree = st.countTokens();
        Double rank;

        boolean firstRun = context.getConfiguration().getBoolean("firstRun", false);
        if (firstRun){
            rank = (1.0/degree) * d * (1.0/n) + (1-d) * (1.0/n);
        }
        else {
            double currentRank = Double.parseDouble(st.nextToken());
            rank = (1.0/degree) * d * currentRank + (1-d) * (1.0/n);
        }

        RankValue rankValue = new RankValue(new DoubleWritable(rank));
        //System.out.println("maprank"+rankValue);
        RankValue outlinkValue;
        try {
            while (st.hasMoreTokens()){
                String outlink = st.nextToken();
                outlinkValue = new RankValue(new Text(outlink));
                //System.out.println("maplink"+outlinkValue);

                context.write(new Text(outlink), rankValue); //write outlink with partial rank component
                context.write(key, outlinkValue); //write key and its outlink
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
