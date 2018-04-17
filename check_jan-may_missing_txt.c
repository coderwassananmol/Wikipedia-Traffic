/* Description: This file checks the if there is missing files from jan-may missing txt files

  Compile Instructions: 
  * gcc -o check_jan-may_missing_txt check_jan-may_missing_txt.c -std=c99
  * ./check_jan-may_missing_txt
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
int main() {
    char date[9];
    time_t t = time(NULL);
    struct tm * ptm = gmtime(&t);

    char year[4];
    char month[2];

    char Test_exist[200];

    FILE *fptr;
    fptr = fopen("jan_may_missing_files.txt", "w");
    if (fptr == NULL)
    {
      printf("Error!\n");
      exit(1);
    }
    int cmd;
      ptm->tm_year = 115;
      ptm->tm_mon = 0;
      ptm->tm_mday = 1;
      mktime(ptm);
      strftime(date, 9, "%Y%m%d", ptm);
      printf("date: %s\n", date);
      memcpy(&year, &date[0], 4);
      year[4] = '\0';
      memcpy(&month, &date[4], 2);
      month[2] = '\0';

      for (int i = 1; i <= 151; i++)
      {
        for (int j = 0; j <= 23; j++)
        {
          if (j <= 9)
          {
            snprintf(Test_exist, 200, "hdfs dfs -test -e ./Wiki/%s-%s/pagecounts-%s-0%d0000.txt", year, month, date, j);
            cmd = system(Test_exist);
          }
          else
          {
            snprintf(Test_exist, 200, "hdfs dfs -test -e ./Wiki/%s-%s/pagecounts-%s-%d0000.txt", year, month, date, j);
            cmd = system(Test_exist);
          }
          if (cmd != 0)
          {
            printf("Missing date: %s-%d\n", date, j);
            fprintf(fptr, "Missing date: %s-%d\n", date, j);
          }
        }
        ptm->tm_mday += 1;
        mktime(ptm);
        strftime(date, 9, "%Y%m%d", ptm);
        memcpy(&year, &date[0], 4);
        year[4] = '\0';
        memcpy(&month, &date[4], 2);
        month[2] = '\0';
      }

    fclose(fptr);
    return 0;
}
