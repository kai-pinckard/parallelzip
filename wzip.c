#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

/*
Performs run length encoding 
*/
int main(int argc, char** argv)
{
    int streak_char;
    int cur_char;
    int count;

    if(argc < 2)
    {
        printf("wzip: file1 [file2 ...]\n");
        exit(1);
    }
    for (int i = 1; i < argc; i++) {
        FILE* file = fopen(argv[i], "r");

        if (!file) {
            perror("wzip: cannot open file");
            exit(1);
        }

        if (i == 1)
        {
            streak_char = fgetc(file);
            cur_char = streak_char;
            count = 0;
        }
        else
        {
            cur_char = fgetc(file);
        }

        while (cur_char != EOF) 
        { 
            if (streak_char == cur_char)
            {
                count++;
                cur_char = fgetc(file);
            }
            else if (cur_char != EOF)
            {
                
                //printf("%i", count);
                if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
                    perror("Can't write to stdout");
                    exit(1);
                }
                printf("%c", streak_char);
                streak_char = cur_char;
                cur_char = fgetc(file);

                count = 1;

            }
        }

        fclose(file);
    }

    //printf("%i", count);
    if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
        perror("Can't write to stdout");
        exit(1);
    }
    printf("%c", streak_char);

    return 0;
}