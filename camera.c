#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <string.h>

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
sem_t empty_slots;      // Camera waits on this
sem_t full_slots;       // Transformer waits on this
sem_t estimation_ready; // Estimator waits on this
sem_t est_done;

// Shared queue
CacheQueue cache;

// Share temporary frame
double *temp_frame;

// Termination flag for sim. To be deleted in final implementation
volatile int should_terminate = 0;

// Provided functions (extern declarations)
extern double *generate_frame_vector(int l);
extern double *compression(double *frame, int length);

// Queue operations
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
    q->front = (q->front + 1) % q->size;
    q->count--;
    return frame;
}

// Camera thread - COMPLETE IMPLEMENTATION
void *camera_thread(void *arg)
{
    int interval = *((int *)arg);
    printf("Camera started with interval %d seconds\n", interval);

    while (1)
    {
        // Generate frame
        double *frame = generate_frame_vector(FRAME_SIZE);

        // Check for termination
        if (frame == NULL)
        {
            printf("Camera: No more frames. Exiting.\n");
            should_terminate = 1;

            // Signal threads to wake up and check termination
            sem_post(&full_slots);       // Wake transformer
            sem_post(&estimation_ready); // Wake estimator

            break;
        }

        // Wait for empty slot in cache
        sem_wait(&empty_slots);

        // Add to queue (protected by mutex)
        pthread_mutex_lock(&queue_mutex);
        enqueue(&cache, frame);
        printf("Camera: Loaded frame into cache. Queue count: %d\n", cache.count);
        pthread_mutex_unlock(&queue_mutex);

        // Signal transformer that frame is available
        sem_post(&full_slots);

        // Simulate camera loading time
        sleep(interval);
    }

    // Signal termination to other threads (you might need additional logic here)
    return NULL;
}

// Transformer thread - ABSTRACT PLACEHOLDER
void *transformer_thread(void *arg)
{
    printf("Transformer started\n");
    // Allocate temp_frame memory once (reused throughout program)
    temp_frame = malloc(FRAME_SIZE * sizeof(double));
    if (!temp_frame)
    {
        perror("malloc for temp_frame");
        return NULL;
    }
    while (!should_terminate || !is_empty(&cache))
    {
        // Wait for frame from camera
        sem_wait(&full_slots);

        // sim termination
        if (should_terminate && is_empty(&cache))
        {
            break; // Exit if no more work
        }

        // Get frame from queue
        // pthread_mutex_lock(&queue_mutex);

        // Check if queue is empty
        if (is_empty(&cache) || cache.frames[cache.front] == NULL)
        {
            printf("Transformer: Queue empty and no more frames. Exiting.\n");
            should_terminate = 1;
            // pthread_mutex_unlock(&queue_mutex);
            sem_post(&estimation_ready); // Wake estimator to exit too
            break;
        }

        pthread_mutex_lock(&temporary_frame_mutex);
        // Copy frame data into pre-allocated temp_frame memory
        memcpy(temp_frame, cache.frames[cache.front], FRAME_SIZE * sizeof(double));
        pthread_mutex_unlock(&temporary_frame_mutex);
        printf("Transformer: Processing frame...\n");

        // Compress the frame (3 seconds)
        printf("Transformer sleeping...\n");
        sleep(3); // Simulate compression time
        printf("Transformer awake after 3 seconds\n");
        sem_wait(&est_done);
        pthread_mutex_lock(&temporary_frame_mutex);
        temp_frame = compression(temp_frame, FRAME_SIZE);
        pthread_mutex_unlock(&temporary_frame_mutex);

        // for (int i = 0; i < FRAME_SIZE; i++) {
        //     printf("%f\n", temp_frame[i]);
        // }

        // Signal estimator that frame is ready for MSE
        sem_post(&estimation_ready);
    }
    free(temp_frame);
    printf("Transformer: Exiting.\n");
    return NULL;
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
void *estimator_thread(void *arg)
{
    printf("Estimator started\n");
    double *compressed = malloc(FRAME_SIZE * sizeof(double));
    double *original = malloc(FRAME_SIZE * sizeof(double));
    if (!compressed || !original)
    {
        perror("malloc");
        return NULL;
    }

    while (!should_terminate || !is_empty(&cache))
    {
        
        if (should_terminate && is_empty(&cache))
        {
            break;
        }

        // Wait for compressed frame from transformer
        sem_wait(&estimation_ready);

        // copy compressed frame
        pthread_mutex_lock(&temporary_frame_mutex);
        memcpy(compressed, temp_frame, FRAME_SIZE * sizeof(double));
        pthread_mutex_unlock(&temporary_frame_mutex);

        // consume original frame from cache
        pthread_mutex_lock(&queue_mutex);
        memcpy(original, dequeue(&cache), FRAME_SIZE * sizeof(double));
        sem_post(&empty_slots);
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&est_done);

        printf("Estimator: Calculating MSE...\n");

        // Calculate MSE (placeholder)
        for (int i = 0; i < FRAME_SIZE; i++)
        {
            printf("Original[%d]=%f, Compressed[%d]=%f\n", i, original[i], i, compressed[i]);
        }
        double mse = calculate_mse(original, compressed, FRAME_SIZE);
        printf("mse = %f\n", mse);

        printf("Estimator: MSE calculated. Queue count: %d\n", cache.count);
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

    // Initialize synchronization primitives
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_mutex_init(&temporary_frame_mutex, NULL);
    sem_init(&empty_slots, 0, CACHE_SIZE); // Start with all slots empty
    sem_init(&full_slots, 0, 0);           // Start with no full slots
    sem_init(&estimation_ready, 0, 0);     // Start with no frames ready for estimation
    sem_init(&est_done, 1, 1);

    // Initialize queue
    init_queue(&cache);

    // Create threads
    pthread_t camera_tid, transformer_tid, estimator_tid;

    pthread_create(&camera_tid, NULL, camera_thread, &interval);
    pthread_create(&transformer_tid, NULL, transformer_thread, NULL);
    pthread_create(&estimator_tid, NULL, estimator_thread, NULL);

    // Wait for threads to complete
    pthread_join(camera_tid, NULL);
    pthread_join(transformer_tid, NULL);
    pthread_join(estimator_tid, NULL);

    // Cleanup
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