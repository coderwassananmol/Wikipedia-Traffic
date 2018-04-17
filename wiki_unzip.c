/*
Explanation:
          The following C code unzips the G-Zip compressed archive from the Hadoop Architecture 
          File System ( HDFS ).
          Input : the time span user would like to unzip
          If the .gz file is missing, a .txt file will be written to indicate the missing Dates.
UpDate:
        One possible future improvment could be that MPI can be used to unzip locally and 
        then transfer the unzipped data to the HDFS.
*/

/* 
  Compile using:
  gcc -o wiki_unzip wiki_unzip.c -std=c99
  ./wiki_unzip
*/

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int main() {
    char Date[9];
    time_t t = time(NULL);
    struct tm * gmtime_ptm = gmtime(&t);

    char year[4];
    char month[2];

    char unzip_file[200];
    char delete_gz_file[200];
    char test[200];

    FILE *fptr;
    fptr = fopen("missing_files2.txt", "w");
    if (fptr == NULL)
    {
      printf("Error!\n");
      exit(1);
    }
    int cmd;
      gmtime_ptm->tm_year = 115; 
      gmtime_ptm->tm_mon = 4; 
      gmtime_ptm->tm_mday = 2; 
      mktime(gmtime_ptm);
      strftime(Date, 9, "%Y%m%d", gmtime_ptm);
      printf("Date: %s\n", Date);
      memcpy(&year, &Date[0], 4);
      year[4] = '\0';
      memcpy(&month, &Date[4], 2);
      month[2] = '\0';

      for (int i = 2; i <= 31; i++)
      {
        for (int j = 0; j <= 23; j++)
        {
          if (j <= 9)
          {
            
            snprintf(unzip_file, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-0%d0000.txt", year, month, Date, j, year, month, Date, j);
            snprintf(delete_gz_file, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, Date, j);
            snprintf(test, 200, "hdfs dfs -test -e /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-0%d0000.gz", year, month, Date, j);
            cmd = system(test);
          }
          else
          {
            snprintf(unzip_file, 200 ,"hadoop fs -text /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz | hadoop fs -put - ./Wiki/%s-%s/pagecounts-%s-%d0000.txt", year, month, Date, j, year, month, Date, j);
            snprintf(delete_gz_file, 200 ,"hdfs dfs -rm /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz", year, month, Date, j);

            snprintf(test, 200, "hdfs dfs -test -e /user/valdiv_n1/Wiki_Data/WikiData/%s-%s/pagecounts-%s-%d0000.gz", year, month, Date, j);
            cmd = system(test);
          }

          if (cmd == 0)
          {
            cmd = system(unzip_file);
            cmd = system(delete_gz_file);
            printf("%s\n", unzip_file);
            printf("%s\n", delete_gz_file);
            printf("\n");
          }
          else
          {
            printf("Missing Date: %s-%d\n", Date, j);
            fprintf(fptr, "Missing Date: %s-%d\n", Date, j);
          }
        }
        gmtime_ptm->tm_mday += 1;
        mktime(gmtime_ptm);
        strftime(Date, 9, "%Y%m%d", gmtime_ptm);
        // printf("Date: %s\n", Date);
        memcpy(&year, &Date[0], 4);
        year[4] = '\0';
        memcpy(&month, &Date[4], 2);
        month[2] = '\0';
      }

    fclose(fptr);
    return 0;
}
