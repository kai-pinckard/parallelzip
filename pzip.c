/*
    Project 2: parallel zip (pzip)
    Completed by: Kai Pinckard

    Note: please link with math "-lm" for math

*/

#include <math.h>
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

// This data structure holds the memory mapped contents of a file and its size.
typedef struct 
{
    const char* name;
    const char* contents;
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

typedef struct 
{
    File_List_t* file_list;
    int start_file;
    int start_index;
    int end_file;
    int end_index;
    // The each thread can store its temporary in its partition's output_list
    Output_List_t output;

} Part_t;

/*
    This function is called by the compression threads and takes a partition as a parameter. 
    The function will compress the bytes specified by the partition and store them in the
    respective thread's partition's output buffer.
*/
void* zip_files(void* void_partition)
{
    Part_t* partition = ((Part_t*) void_partition);
    Data_Pair_t* output_list = partition->output.output_list;

    int count = 0;
    int cur_char = 0;
    int output_pos = 0;
    int end_point = 0;
    int start_point = 0;

    int streak_char = partition->file_list->list[partition->start_file].contents[partition->start_index];

    for (int i = partition->start_file; i <= partition->end_file; i++) 
    {
        const char* file = partition->file_list->list[i].contents;

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
                output_list[output_pos].count = count;
                output_list[output_pos].character = streak_char;
                output_pos++;
                
                streak_char = cur_char;
                count = 1;
            }
        }
    }
    output_list[output_pos].count = count;
    output_list[output_pos].character = streak_char;
    output_pos++;
    partition->output.length = output_pos;
    return NULL;
}

/*
    This function calculates the approximate size in bytes that a partition should be
    based on the total number of bytes to compress and based on this partitions order
    in the list of all the partitions. Later partitions will contain more bytes than
    earlier partitions.
*/
long int calculate_partition_size(long int total_size, int NTHREADS, int partition_number)
{
    long int base_size = (0.4 * total_size) / NTHREADS;
    long int variable_portion = 0.6 * total_size;
    // Since we are using only finitely many terms of an infinite series that sums to 1
    long int error_estimate = variable_portion / (pow(2, NTHREADS));
    variable_portion += error_estimate;
    long int variable_size = variable_portion / (pow(2, NTHREADS - partition_number));
    return base_size + variable_size;
}

/*
    This function divides the bytes that need compressing 
    into NTHREADS partitions so that each thread can be assigned
    a partition of the overall work to complete. 
*/
Part_t* partition_work(File_List_t* file_list, int NTHREADS, long int total_bytes)
{
    Part_t* partition_list = (Part_t*) calloc(NTHREADS, sizeof(Part_t));
    if(!partition_list)
    {
        perror("Failed to allocate space for partition_list");
        exit(1);
    }

    // Number of bytes already assigned in the current partition
    long int assigned_size = 0;
    // The current file who's bytes we are assigning to a partition
    int current_file = 0;
    // The index of the first unassigned byte in the current file
    int current_index = 0;
    // The number of bytes left to assign in the current file.
    int remaining_size = file_list->list[current_file].size;
    // The total number of bytes that have been assigned to partitions
    long int total_assigned = 0;
    // The total number of bytes that should be assigned to the current partition
    long int partition_size;

    // Loop over all the partitions - there is one per thread
    for(int i = 0; i < NTHREADS; i++)
    {
        partition_list[i].file_list = file_list;
        partition_list[i].start_file = current_file;
        partition_list[i].start_index = current_index;

        // The last thread is assigned all remaining work. This is approximately what is calculated by calculate_partition_size
        if (i == NTHREADS - 1)
        {
            partition_size = total_bytes - total_assigned;
            partition_list[i].output.output_list = (Data_Pair_t*) calloc(partition_size, sizeof(Data_Pair_t));
            if(!partition_list[i].output.output_list)
            {
                perror("Failed to allocate space for output_list");
                exit(1);
            }
            partition_list[i].end_file = file_list->length - 1;
            partition_list[i].end_index = file_list->list[file_list->length - 1].size;
        }
        else
        {
            partition_size = calculate_partition_size(total_bytes, NTHREADS, i);
            partition_list[i].output.output_list = (Data_Pair_t*) calloc(partition_size, sizeof(Data_Pair_t));

            for(; current_file < file_list->length; current_file++)
            {
                if(assigned_size + remaining_size < partition_size)
                {
                    assigned_size += remaining_size;
                    total_assigned += remaining_size;

                    // Another file will be added to the current partition
                    // Start reading the next file at the beginning
                    current_index = 0;
                    remaining_size = file_list->list[current_file+1].size;
                }
                else
                {   
                    // This is the last file of the current partition
                    int bytes_needed = partition_size - assigned_size;
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
    return partition_list;
}

int main(int argc, char** argv)
{
    int NTHREADS = 1;

    // The total number of bytes in all the files that need to be compressed
    long int total_bytes = 0;

    // Attempt to get the value of NTHREADS from the environment
    char* nthreads;
    if((nthreads = getenv("NTHREADS")) != NULL)
    {
        NTHREADS = atoi(nthreads);
    }
    
    // Since the name of the program is an argument
    const int file_list_length = argc - 1;
    File_t* list = (File_t*) calloc(file_list_length, sizeof(File_t));
    if(!list)
    {
        perror("Failed to allocate space for file_list list");
        exit(1);
    }
    
    File_List_t file_list = {list, file_list_length};

    if(argc == 1)
    {
        // Although misleading, this is required to pass the wzip tests
        printf("wzip: file1 [file2 ...]\n");
        exit(1);
    }
    else
    {
        // Get file sizes and count total bytes.
        struct stat statbuffer;
        for (int i = 1; i < argc; i++)
        {
            int fd = open(argv[i], O_RDONLY, S_IRUSR | S_IWUSR);
            if(fd == -1)
            {
                perror("Unable to open file");
                exit(1);
            }

            if(fstat(fd, &statbuffer) == -1)
            {
                perror("Unable to get stat info for input file");
                exit(1);
            }

            file_list.list[i-1].name = argv[i];
            file_list.list[i-1].contents = mmap(NULL, statbuffer.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            if(!file_list.list[i-1].contents)
            {
                perror("mmap failed.");
                exit(1);
            }

            file_list.list[i-1].size = statbuffer.st_size;
            total_bytes += statbuffer.st_size;
        }

        // If there is very little data to compress it does not make sense to use multiple threads
        if(total_bytes < 1000)
        {
            NTHREADS = 1;
        }

        // Partition work among each of the threads one partition per thread
        Part_t* partition_list = partition_work(&file_list, NTHREADS, total_bytes);

        pthread_t* threads_list = (pthread_t*) calloc(NTHREADS, sizeof(pthread_t));
        if(!threads_list)
        {
            perror("Failed to allocate space for threads_list");
            exit(1);
        }

        // Create all the threads
        for(int i = 0; i < NTHREADS; i++)
        {
            if(pthread_create(&threads_list[i], NULL, zip_files, (void*) (&partition_list[i])) != 0)
            {
                perror("Unable to create pthread");
                exit(1);
            }
        }

        // Write the output of each of the threads to stdout
        for(int i = 0; i < NTHREADS; i++)
        {
            // This waits for the specific thread to join.
            if(pthread_join(threads_list[i], NULL) != 0)
            {
                perror("Failed to join pthread");
                exit(1);
            }

            Output_List_t output = partition_list[i].output;
            int length = output.length;
            
            // If not the first thread then we need to check the last part of the previous thread's output
            if(i > 0)
            {
                Data_Pair_t previous = partition_list[i-1].output.output_list[partition_list[i-1].output.length - 1];
                if(output.output_list[0].character == previous.character)
                {
                    output.output_list[0].count += previous.count;
                }
                else
                {
                    if(fwrite(&previous.count, sizeof(previous.count), 1, stdout) < 1)
                    {
                        perror("Can't write to stdout");
                        exit(1);
                    }
                    if(putchar(previous.character) == EOF)
                    {
                        perror("Can't write to stdout");
                        exit(1);
                    }  
                }
            }

            for(int j = 0; j < length - 1; j++)
            {  
                if(fwrite(&((output.output_list[j]).count), sizeof((output.output_list[j]).count), 1, stdout) < 1) 
                {
                    perror("Can't write to stdout");
                    exit(1);
                }
                if(putchar(partition_list[i].output.output_list[j].character) == EOF)
                {
                    perror("Can't write to stdout");
                    exit(1);
                }  
            }

            // If this is the last thread then print final character and count, which are usually temporarily omitted
            if( i == NTHREADS - 1)
            {
                if(fwrite(&partition_list[i].output.output_list[length - 1].count, sizeof(partition_list[i].output.output_list[length - 1].count), 1, stdout) < 1) 
                {
                    perror("Can't write to stdout");
                    exit(1);
                }
                if(putchar(partition_list[i].output.output_list[length - 1].character) == EOF)
                {
                    perror("Can't write to stdout");
                    exit(1);
                }  
            }
        }

        free(threads_list);

        // Free the output buffer of each thread.
        for(int i = 0; i < NTHREADS; i++)
        {
            free(partition_list[i].output.output_list); 
        }
        free(partition_list);
    }

    free(file_list.list);
    return 0;
}