/*
 * Group: Group 43
 * Authors: Adham Eltarahoney, Torin Keenan, Steven Abdalla, Tamer Alqasem
 * NetID: U13632134, U44676179, U67196743,  U01278236
 * 
 * Description: Parallel text file compression tool using pthreads.
 * 
 * This implementation parallelizes the compression of multiple text files
 * while maintaining lexicographical output order. Key features:
 * - Fixed-size thread pool (up to 16 workers) to stay under 20 thread limit
 * - Producer-consumer pattern with mutex/condition variables (no busy-waiting)
 * - Results buffered in memory to preserve lexicographical order
 * - Workers capture original file size during read (eliminates double I/O)
 * - Dynamic heap buffer allocation (avoids large stack arrays)
 * - Minimal critical sections for maximum parallel efficiency
 * - Only serial.c and serial.h modified per assignment requirements
 */

 #include <dirent.h> 
 #include <stdio.h> 
 #include <assert.h>
 #include <stdlib.h>
 #include <string.h>
 #include <zlib.h>
 #include <pthread.h>
 
 #define BUFFER_SIZE 1048576 // 1MB
 #define MAX_WORKERS 16      // 16 workers + 1 main = 17 total, under limit of 20
  
 /* 
  * Stores the compressed result for a single file 
  */
 typedef struct {
     unsigned char *data;    // Compressed data buffer
     int comp_size;          // Size of compressed data in bytes
     int orig_size;          // Original uncompressed size in bytes
     int ready;              // Flag: 1 when compression is complete
 } CompressedResult;
  
 /*
  * Represents a compression task for the thread pool
  */
 typedef struct {
     char *full_path;        // Full path to input file
     int index;              // Position in lexicographical order
 } Task;
  
 /*
 * Thread pool state and synchronization objects
  */
 typedef struct {
     pthread_t threads[MAX_WORKERS];
     int num_threads;
     
     Task *tasks;            // Array of tasks (one per file)
     int num_tasks;
     int next_task;          // Index of next task to process
     
     CompressedResult *results;  // Array of results (indexed by file position)
     
     pthread_mutex_t mutex;
     pthread_cond_t task_available;  // Signals when tasks are available
     pthread_cond_t all_done;        // Signals when all tasks complete
     
     int tasks_completed;
     int shutdown;           // Flag to terminate worker threads
 } ThreadPool;
  
  /*
   * Global thread pool instance
   */
  static ThreadPool pool;
  
 /*
  * Worker thread routine: processes compression tasks from the queue
  * 
  * Synchronization Strategy:
  * - Uses minimal critical sections to maximize parallelism
  * - Lock is held ONLY when accessing shared queue state
  * - All expensive operations (I/O, compression) done outside locks
  * - which allows multiple threads to work concurrently without blocking
  * 
  * Work pattern: Lock -> get task -> unlock -> I/O+compress (parrellelization here) -> lock -> store -> unlock
  * 
  * Memory Management:
  * - Allocates buffers on heap to prevent stack overflow and prevent a use-after-free bug
  * - Frees temporary buffers immediately after use
  * - Only compressed result is kept in memory until final write
  */
 static void* worker_thread(void *arg) {
     (void)arg;  // Unused parameter (required by pthread_create signature)
     
     while (1) {
         int task_idx, index;
         char *full_path;
         
         // CRITICAL SECTION 1: Get next task (minimal lock time)
         // Lock is held only long enough to grab the next task index
         pthread_mutex_lock(&pool.mutex);
         
         // Wait for available task or shutdown signal
         while (pool.next_task >= pool.num_tasks && !pool.shutdown) {
             pthread_cond_wait(&pool.task_available, &pool.mutex);
         }
         
         // Check for shutdown
         if (pool.shutdown) {
             pthread_mutex_unlock(&pool.mutex);
             break;
         }
         
         // Grab next task
         task_idx = pool.next_task;
         pool.next_task++;
         index = pool.tasks[task_idx].index;
         full_path = pool.tasks[task_idx].full_path;
         
         pthread_mutex_unlock(&pool.mutex);
         // END CRITICAL SECTION 1
         
         // Perform I/O and compression OUTSIDE lock for maximum parallelism
         // Allocate buffers on heap 
         unsigned char *buffer_in = malloc(BUFFER_SIZE);
         unsigned char *buffer_out = malloc(BUFFER_SIZE);
         assert(buffer_in != NULL && buffer_out != NULL);
         
         
         FILE *f_in = fopen(full_path, "r");
         assert(f_in != NULL);
         int nbytes = fread(buffer_in, 1, BUFFER_SIZE, f_in);
         fclose(f_in);
         
         z_stream strm;
         int ret = deflateInit(&strm, 9);
         assert(ret == Z_OK);
         strm.avail_in = nbytes;
         strm.next_in = buffer_in;
         strm.avail_out = BUFFER_SIZE;
         strm.next_out = buffer_out;
         
         ret = deflate(&strm, Z_FINISH);
         assert(ret == Z_STREAM_END);
         
         int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
         deflateEnd(&strm);
         
         // Allocate the space needed for compressed data
         unsigned char *compressed_data = malloc(nbytes_zipped);
         assert(compressed_data != NULL);
         memcpy(compressed_data, buffer_out, nbytes_zipped);
         
         // Free temporary buffers
         free(buffer_in);
         free(buffer_out);
         
         // CRITICAL SECTION 2: Store result (minimal lock time)
         pthread_mutex_lock(&pool.mutex);
         
         pool.results[index].data = compressed_data;
         pool.results[index].comp_size = nbytes_zipped;
         pool.results[index].orig_size = nbytes;  // Captured during read, no second I/O (key optimization we made)
         pool.results[index].ready = 1;
         pool.tasks_completed++;
         
         // Signal if all tasks are done
         if (pool.tasks_completed == pool.num_tasks) {
             pthread_cond_signal(&pool.all_done);
         }
         
         pthread_mutex_unlock(&pool.mutex);
         // END CRITICAL SECTION 2
     }
     
     return NULL;
 }
  
 /*
  * Comparison function for qsort (lexicographical filename ordering)
  */
 static int cmp(const void *a, const void *b) {
     return strcmp(*(char **) a, *(char **) b);
 }
  
 /*
  * Main compression function: parallelizes per-file compression
  * 
  * Three-Phase Strategy:
  * 
  * PHASE 1 (Serial): Directory scan and sort
  * - Read all .txt filenames from directory
  * - Sort lexicographically to determine output order
  * - This phase is fast, parallelization wouldn't help
  * 
  * PHASE 2 (Parallel): Compression
  * - Spin up thread pool (up to 16 workers)
  * - Workers independently grab tasks, read files, and compress
  * - Results stored in array indexed by sorted position
  * - Compression is CPU-intensive, benefits greatly from parallelism
  * 
  * PHASE 3 (Serial): Output
  * - Write compressed results in lexicographical order
  * - Uses pre-captured file sizes to prevent a second I/O
  * - Order guaranteed by indexed result array
  * 
  * This maximizes parallelism while ensuring correctness as explained in the presentation.
  */
 int compress_directory(char *directory_name) {
     DIR *d;
     struct dirent *dir;
     char **files = NULL;
     int nfiles = 0;
     int capacity = 0;
     
     d = opendir(directory_name);
     if (d == NULL) {
         printf("An error has occurred\n");
         return 0;
     }
     
     //  Phase 1: Directory Scan and Sort (Serial) 
     
     // Collect all .txt files from directory
     while ((dir = readdir(d)) != NULL) {
         int len = strlen(dir->d_name);
         
         // Check if this is a .txt file BEFORE reallocating
         if (len >= 4 && dir->d_name[len-4] == '.' && 
             dir->d_name[len-3] == 't' && 
             dir->d_name[len-2] == 'x' && 
             dir->d_name[len-1] == 't') {
             
             // Grow array only when needed (geometric growth reduces realloc calls)
             if (nfiles >= capacity) {
                 capacity = (capacity == 0) ? 16 : capacity * 2;
                 files = realloc(files, capacity * sizeof(char *));
                 assert(files != NULL);
             }
             
             files[nfiles] = strdup(dir->d_name);
             assert(files[nfiles] != NULL);
             nfiles++;
         }
     }
     closedir(d);
      
     // Sort files lexicographically - this determines final output order
     qsort(files, nfiles, sizeof(char *), cmp);
     
     // Handle empty directory case
     if (nfiles == 0) {
         free(files);
         FILE *f_out = fopen("text.tzip", "w");
         fclose(f_out);
         printf("Compression rate: 0.00%%\n");
         return 0;
     }
     
     //  Phase 2: Parallel Compression Setup 
     
     // Initialize synchronization stuff
     pthread_mutex_init(&pool.mutex, NULL);
     pthread_cond_init(&pool.task_available, NULL);
     pthread_cond_init(&pool.all_done, NULL);
     
     // Initialize thread pool state
     pool.num_tasks = nfiles;
     pool.next_task = 0;
     pool.tasks_completed = 0;
     pool.shutdown = 0;
     
     // Allocate task and result arrays
     // Results array indexed by file position ensures lexicographical order
     pool.tasks = malloc(nfiles * sizeof(Task));
     pool.results = calloc(nfiles, sizeof(CompressedResult));
     assert(pool.tasks != NULL && pool.results != NULL);
     
     // Create tasks with full paths for each file
     for (int i = 0; i < nfiles; i++) {
         int len = strlen(directory_name) + strlen(files[i]) + 2;
         char *full_path = malloc(len * sizeof(char));
         assert(full_path != NULL);
         strcpy(full_path, directory_name);
         strcat(full_path, "/");
         strcat(full_path, files[i]);
         
         pool.tasks[i].full_path = full_path;
         pool.tasks[i].index = i;  // Index preserves sort order
         pool.results[i].ready = 0;
     }
     
     // Create worker threads (bounded by MAX_WORKERS and number of files)
     // Total threads = num_workers + 1 (main) <= 17, well under limit of 20
     pool.num_threads = (nfiles < MAX_WORKERS) ? nfiles : MAX_WORKERS;
     for (int i = 0; i < pool.num_threads; i++) {
         pthread_create(&pool.threads[i], NULL, worker_thread, NULL);
     }
     
     // Wake up all worker threads to begin processing
     pthread_mutex_lock(&pool.mutex);
     pthread_cond_broadcast(&pool.task_available);
     pthread_mutex_unlock(&pool.mutex);
     
     //  Phase 2: Parallel Compression (Workers Active) 
     
     // Wait for all compression tasks to complete
     // Workers signal when last task is done
     pthread_mutex_lock(&pool.mutex);
     while (pool.tasks_completed < pool.num_tasks) {
         pthread_cond_wait(&pool.all_done, &pool.mutex);
     }
     pthread_mutex_unlock(&pool.mutex);
     
     //  shutdown worker threads
     pthread_mutex_lock(&pool.mutex);
     pool.shutdown = 1;
     pthread_cond_broadcast(&pool.task_available);
     pthread_mutex_unlock(&pool.mutex);
     
     // Wait for all threads to exit
     for (int i = 0; i < pool.num_threads; i++) {
         pthread_join(pool.threads[i], NULL);
     }
     
     //  Phase 3: Serial Output (Lexicographical Order) 
     
     // Write compressed results to output file
     // Optimization: No file I/O here - all data already in memory!
     int total_in = 0, total_out = 0;
     FILE *f_out = fopen("text.tzip", "w");
     assert(f_out != NULL);
     
     // Write each compressed file in lexicographical order
     for (int i = 0; i < nfiles; i++) {
         assert(pool.results[i].ready == 1);
         
         // Accumulate totals using pre-captured sizes
         // OPTIMIZATION: orig_size was captured during read, no second file open!
         total_in += pool.results[i].orig_size;
         total_out += pool.results[i].comp_size;
         
         // Write compressed data to output (format: size, then data)
         fwrite(&pool.results[i].comp_size, sizeof(int), 1, f_out);
         fwrite(pool.results[i].data, 1, pool.results[i].comp_size, f_out);
         
         // Free allocated memory as we go
         free(pool.results[i].data);
         free(pool.tasks[i].full_path);
     }
     fclose(f_out);
     
     printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / total_in);
     
     // Clean up all remaining allocations and synchronization primitives
     free(pool.tasks);
     free(pool.results);
     pthread_mutex_destroy(&pool.mutex);
     pthread_cond_destroy(&pool.task_available);
     pthread_cond_destroy(&pool.all_done);
     
     // Free filename list
     for (int i = 0; i < nfiles; i++) {
         free(files[i]);
     }
     free(files);
     
     return 0;
 }
 
 