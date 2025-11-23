#define _POSIX_C_SOURCE 199309L
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include "frames.h"

#define N_ITEMS 5
#define CAMERA_INTERVAL 1
#define TRANSFORM_TIME 3

FrameQueue cache_queue;
FrameQueue est_queue;

sem_t free_slots, full_slots;
sem_t free_est_slots, full_est_slots;

pthread_mutex_t mutex_cache;
pthread_mutex_t mutex_est;
pthread_cond_t cond_cache_data;
pthread_cond_t cond_est_done;

int produced = 0;
int consumed = 0;
int estimated = 0;
struct timespec start_time;

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    long long ms = (ts.tv_sec - start_time.tv_sec) * 1000LL +
                   (ts.tv_nsec - start_time.tv_nsec) / 1000000LL;
    return (double)ms / 1000.0;
}

void print_buffer_state(const char *who, int item, const char *action)
{
    printf("[%.3f] %s %s %d | cache = [", now_ms(), who, action, item);
    for (int i = 0; i < cache_queue.count; i++)
    {
        int idx = (cache_queue.head + i) % CACHE_CAPACITY;
        double *frame = cache_queue.frames[idx];
        printf("%0.0f", frame ? *frame : -1.0);
        if (i < cache_queue.count - 1)
            printf(", ");
    }
    printf("], produced = %d, consumed = %d\n", produced, consumed);
}

void print_est_buffer_state(const char *who, int item, const char *action)
{
    printf("[%.3f] %s %s %d | est_queue = [", now_ms(), who, action, item);
    for (int i = 0; i < est_queue.count; i++)
    {
        int idx = (est_queue.head + i) % CACHE_CAPACITY;
        double *frame = est_queue.frames[idx];
        printf("%0.0f", frame ? *frame : -1.0);
        if (i < est_queue.count - 1)
            printf(", ");
    }
    printf("], consumed = %d, estimated = %d\n", consumed, estimated);
}

// Camera: produce frames into the ring buffer
void *camera(void *arg)
{
    while (1)
    {
        pthread_mutex_lock(&mutex_cache);
        if (produced >= N_ITEMS)
        {
            pthread_mutex_unlock(&mutex_cache);
            break; // stop producing
        }
        pthread_mutex_unlock(&mutex_cache);

        int item = rand() % 100;

        sem_wait(&free_slots);
        pthread_mutex_lock(&mutex_cache);

        if (produced < N_ITEMS)
        {
            double *frame = malloc(sizeof(double));
            if (frame && queue_enqueue(&cache_queue, frame) == 0)
            {
                *frame = (double)item;
                produced++;
                print_buffer_state("Camera", item, "loaded");
                sem_post(&full_slots);
                pthread_cond_signal(&cond_cache_data); // wake transformer
            }
            else
            {
                free(frame);
                // undo wait if enqueue failed
                sem_post(&free_slots);
                pthread_mutex_unlock(&mutex_cache);
                break;
            }
        }
        else
        {
            // undo wait if no more production needed
            sem_post(&free_slots);
            pthread_mutex_unlock(&mutex_cache);
            break;
        }

        pthread_mutex_unlock(&mutex_cache);
        sleep(CAMERA_INTERVAL); // camera capture interval
    }
    return NULL;
}

// Transformer: consume from cache, simulate compression, hand off
void *transformer(void *arg)
{
    while (1)
    {
        pthread_mutex_lock(&mutex_cache);
        bool done = (consumed >= N_ITEMS);
        pthread_mutex_unlock(&mutex_cache);

        if (done)
        {
            break; // stop consuming
        }

        // wait until estimator catches up if we are ahead
        pthread_mutex_lock(&mutex_est);
        while (consumed > estimated)
        {
            pthread_cond_wait(&cond_est_done, &mutex_est);
        }
        pthread_mutex_unlock(&mutex_est);

        pthread_mutex_lock(&mutex_cache);
        while ((consumed >= produced) && produced < N_ITEMS)
        {
            // wait for camera to add a new frame
            pthread_cond_wait(&cond_cache_data, &mutex_cache);
        }

        if (consumed >= produced && produced >= N_ITEMS)
        {
            pthread_mutex_unlock(&mutex_cache);
            break; // nothing more to read
        }

        double *frame = queue_peek(&cache_queue); // read but do not remove; estimator will consume
        int item = frame ? (int)(*frame) : -1;
        consumed++;
        print_buffer_state("Trans", item, "read");

        pthread_mutex_unlock(&mutex_cache);

        // simulate compression: make a new buffer for estimator
        double *compressed = malloc(sizeof(double));
        if (!compressed)
        {
            break;
        }
        *compressed = (double)item; // replace with real compression output as needed

        sleep(TRANSFORM_TIME); // simulate compression time before handing off

        sem_wait(&free_est_slots);
        pthread_mutex_lock(&mutex_est);
        if (queue_enqueue(&est_queue, compressed) == 0)
        {
            sem_post(&full_est_slots);
            print_est_buffer_state("Trans", item, "queued");
        }
        else
        {
            free(compressed);
            sem_post(&free_est_slots); // undo wait if enqueue failed
        }
        pthread_mutex_unlock(&mutex_est);
    }
    return NULL;
}

// Estimator: compare original vs compressed and free cache slot
void *estimator(void *arg)
{
    while (1)
    {
        pthread_mutex_lock(&mutex_est);
        bool done = (estimated >= N_ITEMS);
        pthread_mutex_unlock(&mutex_est);
        if (done)
            break;

        // wait for both original and compressed data
        sem_wait(&full_est_slots);
        sem_wait(&full_slots);

        // consume original frame from cache
        pthread_mutex_lock(&mutex_cache);
        double *original = queue_dequeue(&cache_queue);
        sem_post(&free_slots);
        pthread_mutex_unlock(&mutex_cache);

        // consume compressed frame from estimator queue
        pthread_mutex_lock(&mutex_est);
        double *compressed = queue_dequeue(&est_queue);
        sem_post(&free_est_slots);
        pthread_mutex_unlock(&mutex_est);

        if (compressed && original)
        {
            // compare original vs compressed here
            print_est_buffer_state("Est", (int)(*compressed), "processing");
            free(original);
            free(compressed);
            estimated++;
            pthread_cond_signal(&cond_est_done);
        }
        else
        {
            free(original);
            free(compressed);
        }
    }

    return NULL;
}

int main(void)
{
    pthread_t prod_thread, cons_thread, est_thread;

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    pthread_mutex_init(&mutex_cache, NULL);
    pthread_mutex_init(&mutex_est, NULL);
    pthread_cond_init(&cond_cache_data, NULL);
    pthread_cond_init(&cond_est_done, NULL);
    queue_init(&cache_queue);
    queue_init(&est_queue);

    sem_init(&free_slots, 0, CACHE_CAPACITY);
    sem_init(&full_slots, 0, 0);
    sem_init(&free_est_slots, 0, CACHE_CAPACITY);
    sem_init(&full_est_slots, 0, 0);

    pthread_create(&prod_thread, NULL, camera, NULL);
    pthread_create(&cons_thread, NULL, transformer, NULL);
    pthread_create(&est_thread, NULL, estimator, NULL);

    pthread_join(prod_thread, NULL);
    pthread_join(cons_thread, NULL);
    pthread_join(est_thread, NULL);

    sem_destroy(&free_slots);
    sem_destroy(&full_slots);
    pthread_cond_destroy(&cond_cache_data);
    pthread_cond_destroy(&cond_est_done);
    pthread_mutex_destroy(&mutex_cache);

    return 0;
}


// future changes:
// - done is global flag (NULL returned from frame() function)
// add the compression func
