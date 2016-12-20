import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.dom4j.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sanket on 11/14/16.
 */
public class WikiLinkExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {

    //Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*String content = value.toString();
        Document doc = null;

        try {
            doc = DocumentHelper.parseText(content);
            //Node firstChild = doc.getDocument();
        } catch (DocumentException e) {
            e.printStackTrace();
        }

        String temp = doc.asXML();
        System.out.println(temp);

        String temp2 = doc.selectSingleNode("page").getText();
        System.out.println(temp2);

        String title = doc.selectSingleNode("//page/title").getText();
        String text = doc.selectSingleNode("//page/revision/text").getText();

        if (title.isEmpty() || title == null)
            return;

        Text Title = new Text(title.replace(' ', '_'));
        context.write(Title, new Text("*"));

        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {

            String linkedPage = matcher.group();
            linkedPage = getWikiPageFromLink(linkedPage);
            if (linkedPage == null || linkedPage.isEmpty())
                continue;

            Text foundPage = new Text(linkedPage);

            if (!foundPage.toString().equals(Title.toString()))
                context.write(foundPage, Title);
        }*/

        Text title = new Text(getTitle(value.toString()));
        if (title.toString().isEmpty() || title == null)
            return;

        try {
            context.write(title, new Text("*"));

            for (String link : getLinks(value.toString())){
                if (!link.equals(title.toString()) && !link.isEmpty()){//ignore links to self
                    context.write(new Text(link), title);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static final Pattern LINK_REGEX = Pattern.compile("\\[\\[(.*?)[|(\\]\\])]");//looks for [[this\\notthis]]
    private static final Pattern TITLE_REGEX = Pattern.compile("<title>(.*?)</title>");//looks for <title>this</title>

    private ArrayList<String> getLinks(String xml){
        ArrayList<String> links = new ArrayList<String>();
        Matcher matcher = LINK_REGEX.matcher(xml);
        while(matcher.find())
        {
            String newLink = matcher.group(1);
            links.add(handleWhitespace(newLink));

        }

        links = new ArrayList<String>(new HashSet<String>(links)); //remove duplicates
        return links;
    }


    private String getTitle(String xml){
        String title = new String();
        Matcher matcher = TITLE_REGEX.matcher(xml);

        if (matcher.find())
            title = matcher.group(1);

        return handleWhitespace(title);
    }

    private String handleWhitespace(String s){
        s = s.trim();
        s = s.replaceAll("\\s+","_");//replace all whitespace
        return s;
    }

}

   /* private boolean isNotWikiLink(String link) {
        int start = 1;
        if(link.startsWith("[[")){
            start = 2;
        }

        //if( link.length() < start+2 || link.length() > 100) return true;
        char firstChar = link.charAt(start);

        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;

        if( link.contains(":")) return true; // Matches: external links and translations links
        if( link.contains(",")) return true; // Matches: external links and translations links
        if( link.contains("&")) return true;

        return false;
    }

    private String getWikiPageFromLink(String link){
        if(isNotWikiLink(link)) return null;

        int start = link.startsWith("[[") ? 2 : 1;
        int endLink = link.indexOf("]");

        int pipePosition = link.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }

        int part = link.indexOf("#");
        if(part > 0){
            endLink = part;
        }

        link =  link.substring(start, endLink);
        link = link.replaceAll("\\s", "_");
        link = link.replaceAll(",", "");

        return link;
    }*/
