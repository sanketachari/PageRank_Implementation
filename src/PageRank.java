import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class PageRank {

    static int numOfNodes;


    private static final String NUM_NODES = "/num_nodes.out";
    private static final String ADJACENCY_GRAPH_INTER = "/input_path";
    private static final String ADJACENCY_GRAPH = "/adjacencyGraph.out";
    private static final String PAGE_RANK_OUTPUT_UNSORTED = "/tmp/Unsorted/pageRanksUnsorted.out";
    private static final String PAGE_RANK_OUTPUT_SORTED= "/tmp/Sorted";
    private static final String PAGE_RANK_OUTPUT= "/pageRanksSorted.out";

    // Job 1 -  Extract Wiki links & remove Red links
    public static void parseXml (String arg1, String arg2) throws Exception{


        Configuration conf = new Configuration();
        conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
        conf.set(XMLInputFormat.END_TAG_KEY, "</page>");

        Job job1 = Job.getInstance(conf, "WikiLinks");
        job1.setJarByClass(PageRank.class);

        job1.setInputFormatClass(XMLInputFormat.class);
        job1.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

        job1.setMapperClass(WikiLinkExtractionMapper.class);
        job1.setReducerClass(WikiLinkExtractionReducer.class);

        //job1.setMapOutputKeyClass(Text.class);
        //job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(arg1));
        FileOutputFormat.setOutputPath(job1, new Path(arg2));

        try {
            job1.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Job 2 -  Create adjacency graph of titles and its links
    public static void getAdjacencyGraph (String arg1, String arg2) throws Exception{

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "AdjacencyGraph");

        job2.setJarByClass(PageRank.class);
        job2.setMapperClass(AdjacencyGraphMapper.class);
        job2.setReducerClass(AdjacencyGraphReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(arg1));
        FileOutputFormat.setOutputPath(job2, new Path(arg2));

        try {
            job2.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Job 3 -  Calculate total number of pages/titles
    private static void calTotalPages (String arg1, String arg2) throws Exception{

        Configuration conf = new Configuration();
        Job job3 = Job.getInstance(conf, "N_Calculation");

        job3.setJarByClass(PageRank.class);
        job3.setMapperClass(PageCountMapper.class);
        job3.setReducerClass(PageCountReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(arg1));
        FileOutputFormat.setOutputPath(job3, new Path(arg2));

        try {
            job3.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Job 4 -
    private static void calPageRank (String arg1, String arg2, int iterationNum) throws Exception{

        Configuration conf = new Configuration();
        conf.set("numOfNodes", Integer.toString(numOfNodes));

        if (iterationNum == 0){
            conf.setBoolean("firstRun", true);
        }

        Job job4 = Job.getInstance(conf, "rank");
        job4.setJarByClass(PageRank.class);

        job4.setMapperClass(PageRankMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(RankValue.class);

        job4.setReducerClass(PageRankReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(arg1));
        FileOutputFormat.setOutputPath(job4, new Path(arg2));

        try {
            job4.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Job 5 -
    private static void orderRank(String arg1, String arg2) throws Exception{

        Configuration conf = new Configuration();
        conf.set("numOfNodes", Integer.toString(numOfNodes));

        Job job5 = Job.getInstance(conf, "OrderRank");
        job5.setJarByClass(PageRank.class);

        job5.setMapperClass(SortMapper.class);
        job5.setMapOutputKeyClass(DoubleWritable.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(SortReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(DoubleWritable.class);

        job5.setSortComparatorClass(DescDoubleComparator.class);
        job5.setNumReduceTasks(1);

        job5.setInputFormatClass(KeyValueTextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job5, new Path(arg1));
        FileOutputFormat.setOutputPath(job5, new Path(arg2));

        try {
            job5.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static class DescDoubleComparator extends WritableComparator{

        protected DescDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable d1 = (DoubleWritable) w1;
            DoubleWritable d2 = (DoubleWritable) w2;
            return -d1.compareTo(d2);
        }
    }

    public static void main(String[] args) throws Exception{

        //PageRank.parseXml( args[0], args[1]+ "/tmp/iter0-raw");
        //PageRank.getAdjacencyGraph(args[1] + "/tmp/iter0-raw" , args[1] + "/tmp/iter0");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI(args[1]), conf);

        FileUtil.copyMerge(fs, new Path(args[0]), fs,
                new Path(args[1] + "/tmp/iter0" + ADJACENCY_GRAPH), false, conf, null);

        /*PageRank.calTotalPages( args[1] + "/tmp/iter0" , args[1] + "/tmp/N");

        FileUtil.copyMerge(fs, new Path(args[1] + "/tmp/N"), fs,
                new Path(args[1] + NUM_NODES), false, conf, null);*/

        BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path(args[1]+ NUM_NODES))));
        String line = br.readLine();
        numOfNodes = Integer.parseInt(line);

        int run;
        for(run = 0; run < 8; run++){

            PageRank.calPageRank(args[1] + "/tmp/iter" + String.valueOf(run) ,
                    args[1]+ "/tmp/iter" + String.valueOf(run+1), run);

            if (run == 0) {
                // sort this i.e. first Iteration and store it
                PageRank.orderRank(args[1] + "/tmp/iter" + String.valueOf(run+1),
                        args[1] + PAGE_RANK_OUTPUT_SORTED + "/" + String.valueOf(run + 1));

                FileUtil.copyMerge(fs, new Path (args[1] + PAGE_RANK_OUTPUT_SORTED + "/" + String.valueOf(run + 1)),
                        fs, new Path(args[1] + "/iter1.out"), false, conf, null);
            }

        }

        /*FileUtil.copyMerge(fs, new Path(args[1] + "/tmp/iter" + run),
                fs, new Path(args[1] + PAGE_RANK_OUTPUT_UNSORTED), false, conf, null);*/

        PageRank.orderRank(args[1] + "/tmp/iter" + String.valueOf(run),
                args[1] + PAGE_RANK_OUTPUT_SORTED + "/" + String.valueOf(run));

        FileUtil.copyMerge(fs, new Path(args[1] + PAGE_RANK_OUTPUT_SORTED + "/" + String.valueOf(run)),
                fs, new Path(args[1] + "/iter8.out"), false, conf, null);

    }
}
