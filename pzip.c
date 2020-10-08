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


typedef struct
{
    int count;
    int character;
} Data_Pair_t;


typedef struct
{
    Data_Pair_t* output_list;
    int length;
} Output_List_t;

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
    Output_List_t output;

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
    //printf("here\n");
    Part_t* partition = ((Part_t*) void_partition);
    int streak_char;
    int cur_char;
    int count = 0;
    int end_point;
    int start_point;
    int output_pos = 0;
    Data_Pair_t* output_list = partition->output.output_list;
    //printf("zipping-----------\n");
    //printf("%s", partition.file_list->list[0].contents);
    //print_file_list(partition.file_list);
    streak_char = partition->file_list->list[partition->start_file].contents[partition->start_index];
    //printf("%c", streak_char);

    // NEED TO ADD USE START AND END INDICES IN PARTITION
    // need to fix character counts for repeats in sequential files. 
    for (int i = partition->start_file; i <= partition->end_file; i++) 
    {
        char* file = partition->file_list->list[i].contents;

        if(i == partition->end_file)
        {
            end_point = partition->end_index;
        }
        else
        {
            end_point = partition->file_list->list[i].size;
        }
        if(i == partition->start_file)
        {
            start_point = partition->start_index;
        }
        else
        {
            start_point = 0;
        }

        for(int j = start_point; j < end_point; j++)
        {
            // if we are in the first file.
            cur_char = file[j];
            if (streak_char == cur_char)
            {
                count++;
            }
            else if(cur_char != EOF)
            {
                //printf("tests %i", count);
                /* if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
                    perror("Can't write to stdout");
                    exit(1);
                } */
                //printf("tests %c\n", streak_char);
                output_list[output_pos].count = count;
                output_list[output_pos].character = streak_char;
                output_pos++;
                
                streak_char = cur_char;
                count = 1;
                //printf("\n");
            }
        }
    }
    //printf("testsss %i", count);
    /* if (fwrite(&count, sizeof(count), 1, stdout) < 1) {
        perror("Can't write to stdout");
        exit(1);
    } */
    //printf("testsss %c\n", streak_char);
    //printf("done\n");
    output_list[output_pos].count = count;
    output_list[output_pos].character = streak_char;
    output_pos++;
    //printf("\noutput_pos %d\n", output_pos);
    partition->output.length = output_pos;

    return NULL;
}

/*
    This function should only be called by the main thread to determine the split points for all the other threads. 
*/
Part_t* partition_work(File_List_t* file_list)
{
    // calculate how many bytes each thread should process.
    const int NUM_THREADS = 2;
    long int total_size = 0;
    for(int i = 0; i < file_list->length; i++)
    {
        total_size += file_list->list[i].size;
    }
    //printf("total size: %ld\n", total_size);
    const long int split_size = total_size / NUM_THREADS;
    int remainder = total_size % NUM_THREADS;

    assert(split_size * NUM_THREADS + remainder == total_size);
    //printf("work per thread %ld\n", split_size);
    //printf("Remainder: %d\n", remainder);

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
        partition_list[i].output.output_list = (Data_Pair_t*) calloc(split_size, sizeof(Data_Pair_t));

        if (i == NUM_THREADS -1)
        {
            partition_list[i].end_file = file_list->length - 1;
            partition_list[i].end_index = file_list->list[file_list->length - 1].size;
        }
        else
        {
            //printf("partition: %d\n", i);
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
                    //printf("ran\n");
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
    }

    for(int i = 0; i < NUM_THREADS; i++)
    {
        //printf("\npartition %d\n", i);
        //print_partition(partition_list[i]);
    }

    return partition_list;
}

int main(int argc, char** argv)
{
    const int NUM_THREADS = 2;
    //getenv to load the number of threads.

    // need to have a variable for the last stored char and its count that is globally accessible.

    // Since the name of the program is an argument
    int length = argc - 1;
    File_t* list = (File_t*) calloc(length, sizeof(File_t));
    File_List_t file_list = {list, length};


    if(argc == 1)
    {
        printf("wzip: file1 [file2 ...]\n");
        exit(1);
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

        //print_file_list(file_list);
        Part_t* partition_list = partition_work(&file_list);
        //zip_files(partition_list[0]);
        // Allocate memory to hold a list of pthreads.
        pthread_t* threads_list = (pthread_t*) calloc(NUM_THREADS, sizeof(pthread_t));


        for(int i = 0; i < NUM_THREADS; i++)
        {
             // pthreads return a void pointer and take a void pointer as an argument
            if( 0 != pthread_create(&threads_list[i], NULL, zip_files, (void*) (&partition_list[i])))
            {
                printf("unable to create thread %d", i);
            }
            //printf("%d created\n", i);
        }
        for(int i = 0; i < NUM_THREADS; i++)
        {
            pthread_join(threads_list[i], NULL);
            //printf("\n%d joined\n", i);

            Output_List_t output = partition_list[i].output;
            int length = output.length;
            //printf("\ndsf %d\n", partition_list[0].output.length);
            
            if(i > 0)
            {
                Data_Pair_t previous = partition_list[i-1].output.output_list[partition_list[i-1].output.length - 1];
                // if the first character in the current partition is the same as the final character in the last partition
                if(output.output_list[0].character == previous.character)
                {
                    //printf("ran old: %d, new: %d", output.output_list[0].count, output.output_list[0].count + previous.count);
                    output.output_list[0].count += previous.count;
                }
                else
                {
                    //printf("%d", previous.count);
                    if (fwrite(&previous.count, sizeof(previous.count), 1, stdout) < 1) 
                    {
                        perror("Can't write to stdout");
                        exit(1);
                    }
                    printf("%c", previous.character);
                }
            }
            for(int j = 0; j < length - 1; j++)
            {
                //printf("%d", partition_list[i].output.output_list[j].count);
                if (fwrite(&partition_list[i].output.output_list[j].count, sizeof(partition_list[i].output.output_list[j].count), 1, stdout) < 1) 
                {
                    perror("Can't write to stdout");
                    exit(1);
                }
                printf("%c", partition_list[i].output.output_list[j].character);
            }

            // if we are in the last partition print final characters
            if( i == NUM_THREADS - 1)
            {
                //printf("%d", partition_list[i].output.output_list[length - 1].count);
                if (fwrite(&partition_list[i].output.output_list[length - 1].count, sizeof(partition_list[i].output.output_list[length - 1].count), 1, stdout) < 1) 
                {
                    perror("Can't write to stdout");
                    exit(1);
                }
                printf("%c", partition_list[i].output.output_list[length - 1].character);
            }

            /* if (fwrite(&(partition_list[i].output.output_list), sizeof(Data_Pair_t), partition_list[i].output.length, stdout) < 1) {
                perror("Can't write to stdou t");
                exit(1);
            } */

        }
    }

    // Have the threads write their ouput in a synchronized way
    // uses shared value for final output.

    return 0;
}