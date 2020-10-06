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
    This function should only be called by the main thread to determine the split points for all the other threads. 
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
        printf("file_size: %ld\n", file_list.list[i].size);
    }
}


int main(int argc, char** argv)
{
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