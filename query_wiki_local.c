//gcc -o query_wiki_local query_wiki_local.c -std=c99 -lcurl
//./query_wiki_local ~/Downloads/part-r-00017 ~/hello.txt

/*
Description:

This file will take the cleaned wiki data file as input, and query the wikipedia API to get the corresponding MPI page, then
write to the output file in the format of

            dateHR \t queried_result \t views \t bytes

*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <curl/curl.h>
#include <pthread.h>
#include <unistd.h>

int cmd;

struct string
{
  char *ptr;
  size_t len;
};

void init_string(struct string *s)
{
  s->len = 0;
  s->ptr = malloc(s->len+1);
  if (s->ptr == NULL) {
    fprintf(stderr, "malloc() failed\n");
    exit(EXIT_FAILURE);
  }
  s->ptr[0] = '\0';
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s)
{
  size_t new_len = s->len + size*nmemb;
  s->ptr = realloc(s->ptr, new_len+1);
  if (s->ptr == NULL)
  {
    fprintf(stderr, "realloc() failed\n");
    exit(EXIT_FAILURE);
  }
  memcpy(s->ptr+s->len, ptr, size*nmemb);
  s->ptr[new_len] = '\0';
  s->len = new_len;

  return size*nmemb;
}

// Citation of usage curl library: http://stackoverflow.com/questions/2329571/c-libcurl-get-output-into-a-string
void query_and_writeout(char * dateHR, char * views, char * bytes, char * url, FILE * out_fp)
{
  CURL *curl;
  CURLcode res;
  curl = curl_easy_init();
  if(curl)
  {
    struct string s;
    init_string(&s);

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
    res = curl_easy_perform(curl);
    /* always cleanup */
    curl_easy_cleanup(curl);
    // printf("Query result:\n %s\n", s.ptr);
    fprintf(out_fp, "%s\t%s\t%s\t%s\n", dateHR, s.ptr, views, bytes);
    fsync(1);
    free(s.ptr);
  }
}

void readFiles  (char * fileName_input, char * fileName_output)
{
    FILE * fp; // read file
    FILE *out_fp; // write file

    char line[10000];
    fp = fopen(fileName_input, "r");
    if (fp == NULL)
    {
      printf("Cannot open read0 file Error!\n");
      exit(1);
    }

    out_fp = fopen(fileName_output, "w");
    if (out_fp == NULL)
    {
      printf("Cannot open output file Error!\n");
      exit(1);
    }

    int i;
    int row_index = 1;
    int s_idx_dateHr;
    int e_idx_dateHr;
    int s_idx_name;
    int e_idx_name;
    int s_idx_views;
    int e_idx_views;
    int s_idx_bytes;
    int e_idx_bytes;
    int num_space;

    while (fgets(line, sizeof(line), fp))
    {
      num_space = 0;
      i = 0;
      s_idx_dateHr = 0;
      e_idx_dateHr = 0;
      s_idx_name = 0;
      e_idx_name = 0;
      s_idx_views = 0;
      e_idx_views = 0;
      s_idx_bytes = 0;
      e_idx_bytes = 0;
      // printf("index: %d ", row_index);
      // printf("\n");

      //find each key variable's starting and ending indices
      while (line[i] != '\n')
      {
          if (line[i] == '\t')
          {
            e_idx_dateHr = i-1;
          }
          if (line[i] == ' ')
          {
              num_space += 1;
              if (num_space == 1)
              {
                  s_idx_name = i+1;
              }
              else if (num_space == 2)
              {
                  e_idx_name = i-1;
                  s_idx_views = i+1;
              }
              else
              {
                  e_idx_views = i-1;
                  s_idx_bytes = i+1;
              }
          }
          i+=1;
      }
      e_idx_bytes = i-1;
      row_index += 1;

      char* cat = "<categories>";
      char* _cat = "</categories>";
      char* page_id = "<page _idx=\"";
      char* title_idx = "title=\"Category:";

      int len_dateHR = e_idx_dateHr - s_idx_dateHr + 1;
      int len_name = e_idx_name - s_idx_name + 1;
      int len_views = e_idx_views - s_idx_views + 1;
      int len_bytes = e_idx_bytes - s_idx_bytes + 1;

      char dateHR[len_dateHR + 1];
      char name[len_name + 1];
      char views[len_views+ 1];
      char bytes[len_bytes+ 1];

      // printf("\n");
      memcpy(dateHR, &line[s_idx_dateHr], len_dateHR);
      memcpy(name, &line[s_idx_name], len_name);
      memcpy(views, &line[s_idx_views], len_views);
      memcpy(bytes, &line[s_idx_bytes], len_bytes);
      dateHR[len_dateHR] = '\0';
      name[len_name]= '\0';
      views[len_views]= '\0';
      bytes[len_bytes]= '\0';

      // printf("Begin: \n");
      // printf("%s\n", dateHR);
      // printf("%s\n", name);
      // printf("%s\n", views);
      // printf("%s\n", bytes);
      // printf("\n");
      char url[1000];
      char begin[9] = "https://";
      snprintf(url, 1000, "%sen.wikipedia.org//w/api.php?action=query&format=xml&prop=categories&titles=%s&clshow=!hidden&cllimit=10", begin, name);
      // printf("%s\n", url);

      query_and_writeout(dateHR, views, bytes, url, out_fp);
      if (row_index%50 == 0)
      {
        fflush(out_fp); // any unwritten data in its output file is written to the buffer and clean the RAM.
      }
    }
    fclose(fp);
    fclose(out_fp);
}


int main(int argc, char** argv)
{
  if (argc != 3) {
    exit(-1);
  } else {
  // argv[1]: input file
  // argv[2]: output file
  readFiles(argv[1], argv[2]);
  }
  return 0;
}

// // used for linked pthread
// extern 'C' void queryWiki(char * input, char * output)
// {
//   readFiles(input, output);
// }
