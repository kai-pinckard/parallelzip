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
#include <math.h>

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
    // The thread can store its temporary output here
    Output_List_t output;

} Part_t;


void print_file_list(File_List_t file_list)
{
    for(int i = 0; i < file_list.length; i++)
    {
        printf("filename: %s\n", file_list.list[i].name);
        printf("file contents:\n%s\n", file_list.list[i].contents);
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
}

/*
    This function is called by threads and takes a list of files to be zipped. In addition it takes as a parameter the index into the last
    file that this thread is responsible for so it knows where to stop performing work partway through a file. This function writes to standard out and must check based on
    which thread number it is if it is time for it to be writing to standard out so that the output will come out in the correct order. 
*/
void* zip_files(void* void_partition)
{
    Part_t* partition = ((Part_t*) void_partition);
    Data_Pair_t* output_list = partition->output.output_list;

    int streak_char;
    int cur_char;
    int count = 0;

    int output_pos = 0;

    int end_point = 0;
    int start_point = 0;


    streak_char = partition->file_list->list[partition->start_file].contents[partition->start_index];

    for (int i = partition->start_file; i <= partition->end_file; i++) 
    {
        char* file = partition->file_list->list[i].contents;

        // If this is the first file then the start index is relevant
        if(i == partition->start_file)
        {
            start_point = partition->start_index;
        }
        else
        {
            start_point = 0;
        }

        // If this is the last file then the end_index not file size is relevant
        if(i == partition->end_file)
        {
            end_point = partition->end_index;
        }
        else
        {
            end_point = partition->file_list->list[i].size;
        }

        // Compress the current file into output buffer
        for(int j = start_point; j < end_point; j++)
        {
            cur_char = file[j];
            if (streak_char == cur_char)
            {
                count++;
            }
            else if(cur_char != EOF)
            {
                //printf("tests %i", count);
                //printf("tests %c\n", streak_char);
                output_list[output_pos].count = count;
                output_list[output_pos].character = streak_char;

                // position in our output buffer
                output_pos++;
                
                streak_char = cur_char;
                count = 1;
            }
        }
    }
    //printf("testsss %i", count);
    //printf("testsss %c\n", streak_char);
    output_list[output_pos].count = count;
    output_list[output_pos].character = streak_char;
    output_pos++;
    partition->output.length = output_pos;

    return NULL;
}

/*
    This function calculates the approximate size in bytes that a partition should be
    based on the total number of bytes to compress and based on this partitions order
    in the list of all the partitions.
*/
long int calculate_partition_size(long int total_size, int NTHREADS, int partition_number)
{
    long int base_size = (0.4 * total_size) / NTHREADS;
    long int variable_portion = 0.6 * total_size;
    // Since I am using only finitely many terms of an infinite series that sums to 1
    long int error = variable_portion / (pow(2, NTHREADS));

    variable_portion += error;
    
    long int variable_size = variable_portion / (pow(2, NTHREADS - partition_number));
    return base_size + variable_size;
}

/*
    This function should only be called by the main thread to determine the split points for all the other threads. 
*/
Part_t* partition_work(File_List_t* file_list, int NTHREADS, long int total_bytes)
{
    // Allocate space to store list of partitions
    // check if this fails.
    // this needs to be freed.
    Part_t* partition_list = (Part_t*) calloc(NTHREADS, sizeof(Part_t));

    // Assigned size number of bytes already assigned in the current partition
    long int assigned_size = 0;
    // The current file we are assigning to a partition
    int current_file = 0;
    // The index of the first unassigned byte in the current file
    int current_index = 0;
    // The number of bytes left to assign in the current file.
    int remaining_size = file_list->list[current_file].size;
    // The total number of bytes that have been assigned to partitions
    long int total_assigned = 0;
    // The total number of bytes that should be assigned to the current partition.
    long int split_size;

    // Loop over all the partitions - one per thread.
    for(int i = 0; i < NTHREADS; i++)
    {
        partition_list[i].file_list = file_list;
        partition_list[i].start_file = current_file;
        partition_list[i].start_index = current_index;

        // The last thread is responsible for all remaining work.
        if (i == NTHREADS - 1)
        {
            // need to fix this by keeping track of how many bytes we have processed. 
            split_size = total_bytes - total_assigned;
            //printf("split size: %ld\n", split_size);
            //need to alloc more for error in last file
            partition_list[i].output.output_list = (Data_Pair_t*) calloc(split_size, sizeof(Data_Pair_t));

            partition_list[i].end_file = file_list->length - 1;
            partition_list[i].end_index = file_list->list[file_list->length - 1].size;
        }
        else
        {
            split_size = calculate_partition_size(total_bytes, NTHREADS, i);
            //printf("split size: %ld\n", split_size);
            partition_list[i].output.output_list = (Data_Pair_t*) calloc(split_size, sizeof(Data_Pair_t));

            //printf("partition: %d\n", i);
            for(; current_file < file_list->length; current_file++)
            {
                if(assigned_size + remaining_size < split_size)
                {
                    // Another file will be added to the partition.
                    assigned_size += remaining_size;
                    total_assigned += remaining_size;
                    // Start reading the next file at the beginning
                    current_index = 0;
                    remaining_size = file_list->list[current_file+1].size;
                }
                else
                {   
                    // We are in the last file of the current partition.
                    int bytes_needed = split_size - assigned_size;
                    current_index += bytes_needed;
                    total_assigned += bytes_needed;
                    remaining_size -= bytes_needed;
                    partition_list[i].end_file = current_file;
                    partition_list[i].end_index = current_index;
                    assigned_size = 0;
                    break;
                }
            }
        }
    }

  /*   for(int i = 0; i < NTHREADS; i++)
    {
        printf("\npartition %d\n", i);
        print_partition(partition_list[i]);
    } */

    return partition_list;
}

int main(int argc, char** argv)
{

    int NTHREADS = 1;
    char* nthreads;
    if((nthreads = getenv("NTHREADS")) != NULL)
    {
        NTHREADS = atoi(nthreads);
    }
    
    long int total_bytes = 0;

    // Since the name of the program is an argument
    int file_list_length = argc - 1;
    File_t* list = (File_t*) calloc(file_list_length, sizeof(File_t));
    File_List_t file_list = {list, file_list_length};


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
            total_bytes += statbuffer.st_size;
        }
        //print_file_list(file_list);

        // If there is very little data to compress it does not make sense to use multiple threads
        if(total_bytes < 1000)
        {
            NTHREADS = 1;
        }
        //printf("total size: %ld\n", total_bytes);

        Part_t* partition_list = partition_work(&file_list, NTHREADS, total_bytes);
        pthread_t* threads_list = (pthread_t*) calloc(NTHREADS, sizeof(pthread_t));

        //return 0;
        for(int i = 0; i < NTHREADS; i++)
        {
             // pthreads return a void pointer and take a void pointer as an argument
            if( 0 != pthread_create(&threads_list[i], NULL, zip_files, (void*) (&partition_list[i])))
            {
                printf("unable to create thread %d", i);
            }
        }
        for(int i = 0; i < NTHREADS; i++)
        {
            pthread_join(threads_list[i], NULL);
            Output_List_t output = partition_list[i].output;
            int length = output.length;
            
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
                //printf("here\n");
                //printf("%d", output.output_list[j].count);
                
                if (fwrite(&((output.output_list[j]).count), sizeof((output.output_list[j]).count), 1, stdout) < 1) 
                {
                    perror("Can't write to stdout");
                    exit(1);
                }
                //printf("dsf");
                printf("%c", partition_list[i].output.output_list[j].character);
            }

            // if we are in the last partition print final characters
            if( i == NTHREADS - 1)
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
    return 0;
}