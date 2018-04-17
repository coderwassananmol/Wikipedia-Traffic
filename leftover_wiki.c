/*
  Explanation : This file works for downloading the data from wikimedia traffic data by using MP
*/

/*
  Compile using:
  gcc -o leftover_wiki leftover_wiki.c -std=c99
  ./leftover_wiki
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int main() {
    char date_input[9];
    time_t t = time(NULL);
    struct tm * gmtime_ptm = gmtime(&t);

    char Year[4];
    char Month[2];

    char unzip_file[200];
    char delete_gz_file[200];
    char test[200];
    int cmd;
      gmtime_ptm->tm_Year = 115;
      gmtime_ptm->tm_mon = 9;
      gmtime_ptm->tm_mday = 28; 
      mktime(gmtime_ptm);
      strftime(date_input, 9, "%Y%m%d", gmtime_ptm);
      printf("date_input: %s\n", date_input);
      memcpy(&Year, &date_input[0], 4);
      Year[4] = '\0';
      memcpy(&Month, &date_input[4], 2);
      Month[2] = '\0';
      for (int i = 1; i <= 4; i++)
      {
        for (int j = 0; j <= 23; j++)
        {
          if (j <= 9)
          {
            snprintf(unzip_file, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-0%d0000.txt", Year, Month, date_input, j, Year, Month, date_input, j);
            snprintf(delete_gz_file, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", Year, Month, date_input, j);
          }
          else
          {
            snprintf(unzip_file, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-%d0000.txt", Year, Month, date_input, j, Year, Month, date_input, j);
            snprintf(delete_gz_file, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz", Year, Month, date_input, j);
          }
          cmd = system(unzip_file);
          cmd = system(delete_gz_file);
          printf("%s\n", unzip_file);
          printf("%s\n", delete_gz_file);
          printf("\n");
        }
        gmtime_ptm->tm_mday += 1;
        mktime(gmtime_ptm);
        strftime(date_input, 9, "%Y%m%d", gmtime_ptm);
        memcpy(&Year, &date_input[0], 4);
        Year[4] = '\0';
        memcpy(&Month, &date_input[4], 2);
        Month[2] = '\0';
      }

    return 0;
}
