#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <string.h>
#include <stdatomic.h>

#define CACHE_SIZE 5
#define FRAME_SIZE 8

// Queue structure
typedef struct
{
    double *frames[CACHE_SIZE];
    int front;
    int rear;
    int count;
    int size;
} CacheQueue;

// Synchronization primitives
pthread_mutex_t queue_mutex;
pthread_mutex_t temporary_frame_mutex;
sem_t empty_slots;      // Camera waits on this to make sure there are free slots available so it can fill them.
sem_t full_slots;       // Transformer waits on this so that atleast one slot in the cache is filled with a frame.
sem_t estimation_ready; // Estimator waits on this so that the transformer can finish compressing and storing the frame in the temporary buffer.
sem_t est_done; // Transformer waits on the estimator to finish copying the frame from the temporary buffer before replacing the frame in it.

// The shared queue
CacheQueue cache;

// The shared temporary buffer which holds the compressed frames, one at a time.
double *temp_frame;

// Termination flag (atomic) for safe thread access
atomic_int should_terminate = ATOMIC_VAR_INIT(0);

// Provided functions (extern declarations)
extern double *generate_frame_vector(int l);
extern double *compression(double *frame, int length);

// The queue operation functions
void init_queue(CacheQueue *q)
{
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    q->size = CACHE_SIZE;
    for (int i = 0; i < CACHE_SIZE; i++)
    {
        q->frames[i] = NULL;
    }
}

int is_full(CacheQueue *q)
{
    return q->count == q->size;
}

int is_empty(CacheQueue *q)
{
    return q->count == 0;
}

void enqueue(CacheQueue *q, double *frame)
{
    if (is_full(q))
        return;
    q->frames[q->rear] = frame;
    q->rear = (q->rear + 1) % q->size;
    q->count++;
}

double *dequeue(CacheQueue *q)
{
    if (is_empty(q))
        return NULL;
    double *frame = q->frames[q->front];
    q->frames[q->front] = NULL;  // Remove from queue
    q->front = (q->front + 1) % q->size;
    q->count--;
    return frame;  // Return the dequeued frame to the caller

}

// Camera thread - COMPLETE IMPLEMENTATION
void *camera_thread(void *arg)
{
    int interval = *((int *)arg);
    printf("Camera started with interval %d seconds\n", interval);

    while (1)
    {
        // Generate the frame
        double *frame = generate_frame_vector(FRAME_SIZE);

        // Check if all frames are generated and then terminate
        if (frame == NULL)
        {
            printf("Camera: No more frames. Exiting.\n");
            atomic_store(&should_terminate, 1); // Atomic operation on the thread-safe termination flag

            // Signal the other threads to wake up and check termination
            sem_post(&full_slots);       // Wake up the transformer
            sem_post(&estimation_ready); // Wake up the estimator
            break;
        }

        // Wait for an empty slot in the cache
        sem_wait(&empty_slots);

        // Grab the mutex lock on the queue and then add the frame to it. 
        pthread_mutex_lock(&queue_mutex);
        enqueue(&cache, frame);
        printf("Camera: Loaded frame into cache. Queue count: %d\n", cache.count);
        pthread_mutex_unlock(&queue_mutex);

        // Signal transformer that a new frame is added
        sem_post(&full_slots);

        // Simulate the camera's interval time
        sleep(interval);
    }

    // Terminate the camera thread
    return NULL;
}

// Transformer thread
void *transformer_thread()
{
    printf("Transformer started\n");
    // Allocate memory for temp_frame only once in the start
    temp_frame = malloc(FRAME_SIZE * sizeof(double));
    if (!temp_frame)
    {
        perror("malloc for temp_frame");
        return NULL;
    }
    while (1)
    {
        // Wait for atleast one slot in the cache to be filled.
        sem_wait(&full_slots);
        // Wait for estimator to finish copying the frame from the temporary buffer
        sem_wait(&est_done);

        pthread_mutex_lock(&temporary_frame_mutex); // Acquire the lock on the shared temporary buffer
        pthread_mutex_lock(&queue_mutex); // Acquire the lock on the shared cache queue
        if (is_empty(&cache) && atomic_load(&should_terminate)) // Check if the queue is empty
        {
            printf("Transformer: Queue empty and no more frames. Exiting.\n");
            sem_post(&estimation_ready); // Wake estimator to exit too
            // Release all locks
            pthread_mutex_unlock(&queue_mutex);
            pthread_mutex_unlock(&temporary_frame_mutex);
            break; // Exit the loop to terminate
        }

        // Copy the next frame in the queue to the temporary buffer
        memcpy(temp_frame, cache.frames[cache.front], FRAME_SIZE * sizeof(double));
        pthread_mutex_unlock(&queue_mutex); // Release lock on the queue
        printf("Transformer: Processing frame and going to sleep...\n");
        // Compress the frame (3 seconds)
        sleep(3); // Simulate the compression time
        temp_frame = compression(temp_frame, FRAME_SIZE); // Overwrite the temporary buffer with the compressed frame
        printf("Transformer awake and completed compression.\n");
        pthread_mutex_unlock(&temporary_frame_mutex); // Release lock on the temporary buffer
        sem_post(&estimation_ready); // Signal estimator that compression is done
    }

    printf("Transformer: Exiting.\n");
    return NULL; // Terminate the thread
}

double calculate_mse(double *original, double *compressed, int length)
{
    double mse = 0.0;
    for (int i = 0; i < length; i++)
    {
        double diff = original[i] - compressed[i];
        mse += diff * diff;
    }
    mse /= length;
    return mse;
}

// Estimator thread
void *estimator_thread()
{
    printf("Estimator started\n");
    double *compressed = malloc(FRAME_SIZE * sizeof(double));
    double *original = malloc(FRAME_SIZE * sizeof(double));
    if (!compressed || !original)
    {
        perror("malloc");
        return NULL;
    }

    while (1)
    {
        // Wait for compressed frame from transformer
        sem_wait(&estimation_ready);

        // consume original frame from cache
        pthread_mutex_lock(&queue_mutex);
        if (atomic_load(&should_terminate) && is_empty(&cache))
        {
            printf("Estimator: Queue empty and no more frames. Exiting.\n");
            pthread_mutex_unlock(&queue_mutex);
            sem_post(&est_done);
            break;
        }
        
        double *frame_ptr = dequeue(&cache);
        if (frame_ptr == NULL) {
            /* nothing to consume (race/termination) */
            pthread_mutex_unlock(&queue_mutex);
            sem_post(&est_done);
            continue;
        }
        pthread_mutex_unlock(&queue_mutex);


        /* copy and free the heap buffer produced by generate_frame_vector() */
        memcpy(original, frame_ptr, FRAME_SIZE * sizeof(double));
        free(frame_ptr);
        sem_post(&empty_slots);
        sem_post(&est_done);

        // copy compressed frame
        pthread_mutex_lock(&temporary_frame_mutex);
        memcpy(compressed, temp_frame, FRAME_SIZE * sizeof(double));
        pthread_mutex_unlock(&temporary_frame_mutex);


        printf("Estimator: Calculating MSE...\n");

        /*
        for (int i = 0; i < FRAME_SIZE; i++)
        {
            printf("Original[%d]=%f, Compressed[%d]=%f\n", i, original[i], i, compressed[i]);
        }
        */
        double mse = calculate_mse(original, compressed, FRAME_SIZE);
        printf("mse = %f\n", mse);

        /* read queue count under lock to avoid data race reported by Helgrind */
        int qcount;
        pthread_mutex_lock(&queue_mutex);
        qcount = cache.count;
        pthread_mutex_unlock(&queue_mutex);
        printf("Estimator: MSE calculated. Queue count: %d\n", qcount);
    }
    free(original);
    free(compressed);
    printf("Estimator: Exiting.\n");
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        printf("Usage: %s <interval>\n", argv[0]);
        return 1;
    }

    int interval = atoi(argv[1]);
    if (interval <= 0)
    {
        printf("Interval must be a positive integer.\n");
        return 1;
    }

    // Initialize synchronization primitives
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_mutex_init(&temporary_frame_mutex, NULL);
    sem_init(&empty_slots, 0, CACHE_SIZE); // Start with all slots empty
    sem_init(&full_slots, 0, 0);           // Start with no full slots
    sem_init(&estimation_ready, 0, 0);     // Start with no frames ready for estimation
    /* pshared = 0 for semaphores used between threads in same process */
    sem_init(&est_done, 0, 1); // Initialized to 1 as we do not want transformer to be waiting for the estimator on the first frame

    // Initialize the cache queue
    init_queue(&cache);

    // Create the threads
    pthread_t camera_tid, transformer_tid, estimator_tid;

    pthread_create(&camera_tid, NULL, camera_thread, &interval);
    pthread_create(&transformer_tid, NULL, transformer_thread, NULL);
    pthread_create(&estimator_tid, NULL, estimator_thread, NULL);

    // Wait for the threads to complete
    pthread_join(camera_tid, NULL);
    pthread_join(transformer_tid, NULL);
    pthread_join(estimator_tid, NULL);

    // Cleanup all semaphores, mutexes and temporary buffer
    free(temp_frame);
    pthread_mutex_destroy(&queue_mutex);
    pthread_mutex_destroy(&temporary_frame_mutex);
    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
    sem_destroy(&estimation_ready);
    sem_destroy(&est_done);

    printf("All frames processed. Program terminated.\n");
    return 0;
}