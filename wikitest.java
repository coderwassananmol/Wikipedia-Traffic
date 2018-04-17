package anmol.data.analytics.testwiki;

import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.Object;
import java.lang.String;

public class TestWiki
{
    public static void main( String[] args )
    {
        String file_name = "part-r-00017";
        String read_line = null;
        try
        {
            FileReader fileReader = new FileReader(file_name);

            BufferedReader bufferedReader = new BufferedReader(fileReader);

            int index = 1;
            while ((read_line = bufferedReader.readLine()) != null)
            {
              System.out.println(index);
              String name, views, dateHR, bytes, LineList[], Line[];

              Line = read_line.trim().split("\\t");
              dateHR = Line[0];

              LineList = Line[1].trim().split("\\s");
              name = LineList[1];//
              views = LineList[2];
              bytes = LineList[3];
              /*
               * action = query
               * format = json          --> Output Format
               * prop = categories      --> desire proterty
               * title = name           --> from the read_line in the input file
               * clshow = !hidden       --> public categories
               * cllimit = 5            --> only the first 5 categories
               */
              String sentence, cat, _cat, page_id, title_idx, entry, _value, urlCmd, notHidden;
              sentence = "";
              cat = "<categories>";
              _cat = "</categories>";
              page_id = "<page _idx=\"";
              title_idx = "title=\"Category:";
              notHidden = "&clshow=!hidden&cllimit=1";

              try {
                  //Using Wikipedia API
                 URL url = new URL("https://en.wikipedia.org//w/api.php?action=query&format=xml&prop=categories&titles=" + name + notHidden);
                 Scanner s = new Scanner(url.openStream());
                 s.useDelimiter("\\n");
                 sentence = s.next();
                 int s_indx, e_indx;
                 s_indx = sentence.indexOf(page_id);

                 if (s_indx != -1) 
                 {
                   s_indx += page_id.length();
                   e_indx = sentence.indexOf("\"", s_indx);
                   String idx = sentence.substring(s_indx, e_indx);
                   String negtive_one = "-1";
                   if (!idx.equals(negtive_one))
                   {
                     int s_cat, e_cat;
                     s_cat = sentence.lastIndexOf(cat);
                     if (s_cat != -1)
                     {
                       s_cat += cat.length();
                       e_cat = sentence.indexOf(_cat);

                       if (e_cat < sentence.length())
                       {
                         String categories = sentence.substring(s_cat, e_cat);
                         String categoryList = "";
                         int s_title = 1;
                         int e_title = 1;
                         int flag = 0;
                         while(flag != -1)
                         {
                           flag = categories.indexOf(title_idx, e_title);
                           if (flag != -1) {
                             s_title = flag + title_idx.length();
                             e_title = categories.indexOf("\"", s_title);
                             String category = categories.substring(s_title, e_title);
                             categoryList += (category + "\t");
                           }
                         }

                        _value = "name: " + name + " --- categories: " + categoryList;
                        System.out.println(_value);
                       }
                     }
                   }
                 }
                index += 1;
              }
              catch(IOException ex) {
                 ex.printStackTrace();
              }
            }
            close(bufferedReader);
        }
        catch(FileNotFoundException ex)
        {
          System.out.println("Cannot open file: '" + file_name + "'");
        }
        catch(IOException ex)
        {
            System.out.println("Error reading file: '" + file_name + "'");
        }
    }
}

