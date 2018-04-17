/*
 Description:
 In this file, we mainly calculated the percentage of party-line for each year.

 In stage 1:
 mapper <year.rollNum, PartyVote>   --->>    Reducer <year.rollNum, if_partyline>

 In stage 2:
 mapper <year, if_partyline>    --->>  Reducer <year, percentage>

 We can see two stages' outputs from:
 http://w01:50070/explorer.html#/user/li_j8/PartyLine

 Run:
 hadoop jar target/partyline-1.0-SNAPSHOT.jar /user/valdiv_n1/partyLine/* ./PartyLine/stage1_output ./PartyLine/stage2_output
 hadoop jar target/cleandata-1.0-SNAPSHOT.jar /user/valdiv_n1/TESTWIKI/INPUT/Wiki/2015-01/ /user/valdiv_n1/TESTWIKI/OUTPUT
 hadoop jar target/wikileatics-1.0-SNAPSHOT.jar /user/valdiv_n1/TESTWIKI/INPUT/ /user/valdiv_n1/TESTWIKI/OUTPUT
 */

package edu.denison.cs345.wikileatics;

import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikiLeatics {
    // **note: Files' content are white character separated. use \w in the regEx**
    public static class Map_1 extends Mapper<LongWritable, Text, Text, Text> {
        //private final static FloatWritable one = new IntWritable(1);
        private Text KEY = new Text();
        private Text VALUE = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line;
            line = value.toString();           //Convert a line to string
            //====================== Split information in a line by whitespace characters into a list ============================
            String name, views, dateHR, bytes, LineList[], Line[];

            Line = line.trim().split("\\t");  //splits the line by spaces and solve issues if there are many spaces
            dateHR = Line[0];

            LineList = Line[1].trim().split("\\s");
            name = LineList[1];//
            views = LineList[2];
            bytes = LineList[3];
            // More work will be done iff the language encoding is  en, En, En.d, and en.n
            /*
             * action = query
             * format = json          --> Output Format
             * prop = categories      --> desire proterty
             * title = name           --> from the line in the input file
             * clshow = !hidden       --> public categories
             * cllimit = 5            --> only the first 5 categories
             */
            //in a line of the input file we extract the name:
            String sentence, cat, _cat, page_id, title_idx, entry, _value, urlCmd, notHidden;
            sentence = "";
            cat = "<categories>";
            _cat = "</categories>";
            page_id = "<page _idx=\"";
            title_idx = "title=\"Category:";
            //CONSTANT String the indicates the entry of the API
            notHidden = "&clshow=!hidden&cllimit=5";

            try {
               //Complete URL that indicates the action we need from the WikiMedia DB:
               URL url = new URL("https://en.wikipedia.org//w/api.php?action=query&format=xml&prop=categories&titles=" + name + notHidden);
               Scanner s = new Scanner(url.openStream());
               s.useDelimiter("\\n");
               sentence = s.next();
               //========== index of the start and end of the page index =============
               int s_indx, e_indx;
               s_indx = sentence.indexOf(page_id);

               if (s_indx != -1) // check if the XML contains page _idx.
               {
                 s_indx += page_id.length();
                 e_indx = sentence.indexOf("\"", s_indx);
                 String idx = sentence.substring(s_indx, e_indx);
                 //  int indexNumber = Integer.parseInt(try_it);
                 String negtive_one = "-1";
                 if (!idx.equals(negtive_one))   // check if page _idx != -1 --> if it is a valid page
                 {
                   //========== index of the start and end of the Category List ==========
                   int s_cat, e_cat;
                   s_cat = sentence.lastIndexOf(cat);
                   if (s_cat != -1)     // checks whether the valid page has categories
                   {
                     s_cat += cat.length();
                     e_cat = sentence.indexOf(_cat);

                     if (e_cat < sentence.length())
                     {
                       String categories = sentence.substring(s_cat, e_cat);
                       //====================== Split information in a line by whitespace characters into a list ============================
                       String categoryList = "";
                       int s_title = 1;
                       int e_title = 1;
                       int flag = 0;
                       while(flag != -1) // Iterates through the categories --> if it has less than 5 jumps out.
                       {
                         flag = categories.indexOf(title_idx, e_title);
                         if (flag != -1) {
                           s_title = flag + title_idx.length();
                           e_title = categories.indexOf("\"", s_title);
                           String category = categories.substring(s_title, e_title);
                           categoryList += (category + "\t");
                         }
                       }

                       _value = categoryList + views + "\t" + bytes;
                       //  set KET and VALUE
                       KEY.set(dateHR);
                       VALUE.set(_value);
                       //COMMIT KET and VALUE
                       context.write(KEY, VALUE);
                     }
                   }
                 }
               }
              // KEY.set(dateHR);
              // VALUE.set(idx);
              //
              // //COMMIT KET and VALUE
              // context.write(KEY, VALUE);

            }
            catch(IOException ex) {
               ex.printStackTrace(); // for now, simply output it.
            }

        }
    }

    public static class Reduce_1 extends Reducer<Text, Text, Text, Text> {
        private Text VALUE = new Text();
        //private final static IntWritable one = new IntWritable(1);
        //private final static IntWritable zero = new IntWritable(0);
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

            for (Text val : values)
            {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "wikileatics");
    job.setJarByClass(WikiLeatics.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map_1.class);
    job.setReducerClass(Reduce_1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true); // Can't start until the first stage is finished

  }
}
