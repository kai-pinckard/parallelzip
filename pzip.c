#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

/*
    the tricky thing about this assignment is that multiple files need to be compressed into a single file with runs between files being combined.
    Thus it is only safe to split a file between threads for work when there is a change of character for example "aaaab" can be split so that one thread will recieve all the 'a's
    and another thread will receive the b. Furthermore, the begginnings and ends of files must be checked for this kind of pattern as well it is not safe to simply send 
    different files to different threads without checking for this kind of repitition separation. 
*/

// This data structure contains a list of files and the last position to process
// in the last file.
struct Part_t
{
    // a list containing the contents of all the files to be processed. 
    char** files_list;
    // The index in the last file to stop processing.
    long long int end_position;
};

// This data structure holds the memory mapped contents of a file and its size.
typedef struct 
{
    char* name;
    char* contents;
    long int size;
} File_t;

typedef struct 
{
    File_t* list;
    int length;
} File_List_t;

/*
    This function is called by threads and takes a list of files to be zipped. In addition it takes as a parameter the index into the last
    file that this thread is responsible for so it knows where to stop performing work partway through a file. This function writes to standard out and must check based on
    which thread number it is if it is time for it to be writing to standard out so that the output will come out in the correct order. 
*/
void zip_files()
{

}

/*
This helper function for partition work returns the index of the second type of character occuring in
a file for example in the file aaaabccc it would return the index of b.

desired_split_index refers to the position of the file which we desire the split to occur in
that is contained in the list of files contained in the file_list struct. 
returns a pointer to the last character that is to be included in the current split.




a much simpler way to split is to instead split whereever i would like to based on fairness and then when the threads are writing to stdout they can check the last character that
was written and if it matches the current character it can add that count to its current count and then write the data to standard out. alternatively there could be a communication variable to 
hold the last value in the current threads chunk in this way standard out does not need to be modified. 
*/
char* get_split_index(File_List_t file_list, int desired_split_index)
{
    char* split_file = file_list.list[desired_split_index];
    long int size = file_list.list[desired_split_index].size;

    int first_char = split_file[0];
    
    for(long int i = 1; i < size; i++)
    {
        if (split_file[i] != first_char)
        {
            return i;
        }
    }

    // if there are more files we must continue searching for a good split.
}
/*
    This function should only be called by the main thread to determine the split points for all the other threads. 

    IMPORTANT NOTE even if only one character is repeating in multiple consecutive files that character must still be summed correctly without repeated counts.
*/
struct Part_t* partition_work(char** files_to_partition)
{
    return NULL;
}


void print_file_list(File_List_t file_list)
{
    for(int i = 0; i < file_list.length; i++)
    {
        printf("filename: %s\n", file_list.list[i].name);
        //printf("file contents:\n%s\n", file_list.list[i].contents);
        printf("file_size: %ld\n", file_list.list[i].size);
    }
    printf("\nlist length: %d\n", file_list.length);
}

/* 
File_List_t init_file_list(int length)
{

    return 
}
 */

int main(int argc, char** argv)
{
    const int NUM_THREADS = 1;
    //getenv to load the number of threads.

    // Since the name of the program is an argument
    int length = argc - 1;
    File_t list[length];
    File_List_t file_list = {list, length};

    if(argc == 1)
    {
        printf("Error no input files specified.\n");
    }
    else
    {
        // Get file sizes
        for (int i = 1; i < argc; i++)
        {
            int fd = open(argv[i], O_RDONLY, S_IRUSR | S_IWUSR);
            struct stat statbuffer;
            if(fstat(fd, &statbuffer) == -1)
            {
                printf("Error unable to get stat info for file %s\n", argv[i]);
                return 1;
            }
            //printf("%s file size %ld\n", argv[i], statbuffer.st_size);

            file_list.list[i-1].name = argv[i];
            // check for mmap fail
            file_list.list[i-1].contents = mmap(NULL, statbuffer.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            file_list.list[i-1].size = statbuffer.st_size;
        }

        print_file_list(file_list);
    }
    
    
    // Map all of the files into shared memory
    

    // partion work among the threads

    // Have the threads write their ouput in a synchronized way

    return 0;
}