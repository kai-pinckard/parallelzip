#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>


/*
    Instead of writing the final character and its count in a given partition to standard out
    the thread will store it here so that the next thread can use it to initialize its initial
    character and count.
*/
int shared_final_count = 0;
int shared_final_char = 0;
int partitions_completed = 0;







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

//REMEMBER TO CONST THE FILE_LIST DATA STRUCTURE

// This data structure contains a list of files and the last position to process
// in the last file.
typedef struct 
{
    File_List_t* file_list;

    // Index in file_list.list for first file in this partition
    int start_file;
    // First index in start_file to process
    int start_index;

    // Index in file_list.list for the last file in this partition
    int end_file;
    // last index in end_file in to process
    int end_index;
    // The position of this partition in the ordering of partitions starting at 0
    int partition_number;
    // The thread can store its temporary output here
    char* output_buffer;

} Part_t;


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


void print_partition(Part_t partition)
{
    printf("start file index: %d\n", partition.start_file);
    printf("start index: %d\n", partition.start_index);
    printf("end file index: %d\n", partition.end_file);
    printf("end index: %d\n", partition.end_index);
    printf("partition number: %d\n", partition.partition_number);
}



/*
    This function is called by threads and takes a list of files to be zipped. In addition it takes as a parameter the index into the last
    file that this thread is responsible for so it knows where to stop performing work partway through a file. This function writes to standard out and must check based on
    which thread number it is if it is time for it to be writing to standard out so that the output will come out in the correct order. 
*/
void* zip_files(void* void_partition)
{
    printf("here\n");
    Part_t partition = *((Part_t*) void_partition);
    int streak_char;
    int cur_char;
    int count = 0;
    int end_point;
    //printf("zipping-----------\n");
    //printf("%s", partition.file_list->list[0].contents);
    //print_file_list(partition.file_list);
    streak_char = partition.file_list->list[0].contents[0];
    //printf("%c", streak_char);

    // NEED TO ADD USE START AND END INDICES IN PARTITION
    // need to fix character counts for repeats in sequential files. 
    for (int i = partition.start_file; i <= partition.end_file; i++) 
    {
        char* file = partition.file_list->list[i].contents;

        if(i == partition.end_file)
        {
            end_point = partition.end_index;
        }
        else
        {
            end_point = partition.file_list->list[i].size;
        }

        for(int j = partition.start_index; j < end_point; j++)
        {
            // if we are in the first file.
            cur_char = file[j];
            if (streak_char == cur_char)
            {
                count++;
            }
            else if(cur_char != EOF)
            {
                printf("%i", count);
                /* if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
                    perror("Can't write to stdout");
                    exit(1);
                } */
                printf("%c", streak_char);
                streak_char = cur_char;
                count = 1;
                printf("\n");
            }
        }
    }
    printf("%i", count);
    /* if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
        perror("Can't write to stdout");
        exit(1);
    } */
    printf("%c", streak_char);


    while(partition_number != partition.partition_number)
    {
        //sleep
        pthread_sleep
    }




    return NULL;
}

/*
    This function should only be called by the main thread to determine the split points for all the other threads. 

    IMPORTANT NOTE even if only one character is repeating in multiple consecutive files that character must still be summed correctly without repeated counts.
*/
Part_t* partition_work(File_List_t* file_list)
{
    // calculate how many bytes each thread should process.
    const long int NUM_THREADS = 1;
    long int total_size = 0;
    for(int i = 0; i < file_list->length; i++)
    {
        total_size += file_list->list[i].size;
    }
    printf("total size: %ld\n", total_size);
    long int split_size = total_size / NUM_THREADS;
    int remainder = total_size % NUM_THREADS;

    assert(split_size * NUM_THREADS + remainder == total_size);
    printf("work per thread %ld\n", split_size);
    printf("Remainder: %d\n", remainder);

    // Allocate space to store list of partitions
    // check if this fails.
    // this needs to be freed.
    Part_t* partition_list = (Part_t*) calloc(NUM_THREADS, sizeof(Part_t));

    long int assigned_size = 0;
    int current_file = 0;
    int current_index = 0;
    int remaining_size = file_list->list[current_file].size;

    for(int i = 0; i < NUM_THREADS; i++)
    {
        partition_list[i].file_list = file_list;
        partition_list[i].start_file = current_file;
        partition_list[i].start_index = current_index;
        partition_list[i].partition_number = i;
        // need to free
        partition_list[i].output_buffer = (char*) calloc(5*split_size, sizeof(char));

        for(; current_file < file_list->length; current_file++)
        {
            if(assigned_size + remaining_size < split_size)
            {
                // Another file will be added to the partition.
                assigned_size += remaining_size;
            }
            else if(assigned_size + remaining_size == split_size)
            {
                // This file is exactly the number of bytes we want.
                current_index = file_list->list[current_file].size;
                partition_list[i].end_file = current_file;
                partition_list[i].end_index = current_index;
                current_file++;
                break;
            }
            else
            {
                // We are in the last file of the current partition.
                int bytes_needed = split_size - assigned_size;
                current_index = bytes_needed;
                remaining_size -= bytes_needed;
                partition_list[i].end_file = current_file;
                partition_list[i].end_index = current_index;
                break;
            }
            // possibly check for out of bounds.
            remaining_size = file_list->list[current_file+1].size;
        }
        assigned_size = 0;
    }

    for(int i = 0; i < NUM_THREADS; i++)
    {
        printf("\npartition %d\n", i);
        print_partition(partition_list[i]);
    }

    return partition_list;
}

int main(int argc, char** argv)
{
    const int NUM_THREADS = 1;
    //getenv to load the number of threads.

    // need to have a variable for the last stored char and its count that is globally accessible.

    // Since the name of the program is an argument
    int length = argc - 1;
    File_t* list = (File_t*) calloc(length, sizeof(File_t));
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
            // check for mmap fail                                            update to mapp shared
            file_list.list[i-1].contents = mmap(NULL, statbuffer.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            file_list.list[i-1].size = statbuffer.st_size;
        }

        print_file_list(file_list);
        Part_t* partition_list = partition_work(&file_list);
        //zip_files(partition_list[0]);

        void* part_arg = (void*) (&partition_list[0]);
        // Allocate memory to hold a list of pthreads.
        pthread_t* threads_list = (pthread_t*) calloc(NUM_THREADS, sizeof(pthread_t));

        for(int i = 0; i < NUM_THREADS; i++)
        {
             // pthreads return a void pointer and take a void pointer as an argument
            if( 0 != pthread_create(&threads_list[i], NULL, zip_files, part_arg))
            {
                printf("unable to create thread %d", i);
            }
            printf("%d created\n", i);
        }
        for(int i = 0; i < NUM_THREADS; i++)
        {
            pthread_join(threads_list[i], NULL);
            printf("\n%d joined\n", i);
        }
    }

    // Have the threads write their ouput in a synchronized way
    // uses shared value for final output.

    return 0;
}