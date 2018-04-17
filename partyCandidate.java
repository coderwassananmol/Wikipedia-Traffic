/*
Explanation:
  In this file, we mainly calculated the percentage of party-line for each year.

  * In stage 1:
    mapper <year.rollNum, PartyVote>   --->>    Reducer <year.rollNum, if_partyline>

  * In stage 2:
    mapper <year, if_partyline>    --->>  Reducer <year, percentage>

  * We can see two stages' outputs from:
    http://w01:50070/explorer.html#/user/li_j8/PartyLine

  * Run:
    hadoop jar target/partyline-1.0-SNAPSHOT.jar /user/valdiv_n1/partyLine/* ./PartyLine/stage1_output ./PartyLine/stage2_output
*/

package anmol.data.analytics.testwiki;

import java.io.IOException;
import java.util.*;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartyLine {

   public static class Map_1 extends Mapper<LongWritable, Text, Text, Text> {
      //private final static FloatWritable one = new IntWritable(1);
      private Text KEY = new Text();
      private Text VALUE = new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

         String line, validator, partyFlag, voteFlag, begin;

         // Get the path
         FileSplit fileSplit = (FileSplit)context.getInputSplit();
         Path filename = fileSplit.getPath();

         validator = "<recorded-vote>";
         partyFlag = "party=\"";
         voteFlag = "<vote>";
         line = value.toString();
         begin = line.substring(0, Math.min(line.length(), 15));

         if (begin.equals(validator))
         {
            int party_s, vote_s;
            String path[], rollNum, _key, PARTY, VOTE, _value;

            // Compute indexes of the value locations
            party_s = line.indexOf(partyFlag) + partyFlag.length();
            vote_s = line.indexOf(voteFlag) + voteFlag.length();

            path = (filename.toString()).split("/");

            // Get the roll number from our path list. We don't want the "roll" part and the ".xml" part
            rollNum = path[7].substring(4, 7);

            // Our key is in the format of "year.rollNum".
            _key = path[6] +  "." + rollNum;

            // Find this person's party and its vote
            PARTY = line.substring(party_s, party_s+1);
            VOTE = line.substring(vote_s, vote_s+1);
            _value = PARTY + VOTE;

            KEY.set(_key);
            VALUE.set(_value);
            context.write(KEY, VALUE);
         }
      }
   }

   public static class Reduce_1 extends Reducer<Text, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private final static IntWritable zero = new IntWritable(0);

      public void reduce(Text key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException {

         // The following 4 variables are used to track the count of each outcomes
         int Rep_y = 0;
         int Rep_n = 0;
         int Dem_y = 0;
         int Dem_n = 0;
         for (Text val : values)
         {
            String V = val.toString();
            // The party and vote here matches the value that is from mapper
            String party = V.substring(0,1);
            String vote = V.substring(1,2);
            if (party.equals("R"))
            {
              if (vote.equals("Y"))
              {
                Rep_y += 1;
              }
              else
              {
                Rep_n += 1;
              }
            }
            else if (party.equals("D"))
            {
              if (vote.equals("Y"))
              {
                Dem_y += 1;
              }
              else
              {
                Dem_n += 1;
              }
            }
         }
         // Calculate the majority vote for each party
         int Rep = Rep_y - Rep_n;
         int Dem = Dem_y - Dem_n;

         // If the following condition is true, then it is a party line; Otherwise it is not.
         if ((Rep < 0 && Dem > 0) || (Rep > 0 && Dem < 0))
         {
           context.write(key, one);
         }
         else
         {
           context.write(key, zero);
         }
      }
   }

   public static class Map_2 extends Mapper<LongWritable, Text, Text, IntWritable> {
     private Text KEY = new Text();
     private final static IntWritable one = new IntWritable(1);
     private final static IntWritable zero = new IntWritable(0);

     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line, year, val;
       line = value.toString();
       year = line.substring(0,4);
       int tabIndx = line.indexOf("\t");
       val = line.substring(tabIndx+1, tabIndx+2); // Get the 0 or 1 from each year's roll call.
       KEY.set(year);
       if (val.equals("1"))
       {
         context.write(KEY, one);
       }
       else
       {
         context.write(KEY, zero);
       }
     }
   }

   public static class Reduce_2 extends Reducer<Text, IntWritable, Text, FloatWritable> {

     public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int PL = 0;
        int len = 0;
        for (IntWritable val : values)
        {
          PL += val.get(); // Get the sum of the yes
          len += 1; // Get the total count of votes
        }
        float percent = (((float)PL/len) * 100);
        context.write(key, new FloatWritable(percent));
      }

   }
   public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();

     // Job 1 is charge of the first stage
     Job job = new Job(conf, "partyline");
     job.setJarByClass(PartyLine.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);

     job.setMapperClass(Map_1.class);
     job.setReducerClass(Reduce_1.class);
     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
     Path OUTPUT_DIR = new Path(args[1]); // Used for the following second stage's input path
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, OUTPUT_DIR);

     job.waitForCompletion(true); // Can't start until the first stage is finished

     // Job 2 is charge of the first stage
     Job job2 = new Job(conf, "partyline2");
     job2.setJarByClass(PartyLine.class);
     job2.setOutputKeyClass(Text.class);
     job2.setOutputValueClass(IntWritable.class);

     job2.setMapperClass(Map_2.class);
     job2.setReducerClass(Reduce_2.class);
     job2.setInputFormatClass(TextInputFormat.class);
     job2.setOutputFormatClass(TextOutputFormat.class);

     FileInputFormat.addInputPath(job2, OUTPUT_DIR);
     FileOutputFormat.setOutputPath(job2, new Path(args[2]));
     job2.waitForCompletion(true);

   }

}
