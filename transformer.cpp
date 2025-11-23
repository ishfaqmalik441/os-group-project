#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <cstring>
#include <queue>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

using namespace std;

#define FRAME_LENGTH 8
sem_t transformer_sem;

// Global queue to store frames
queue<double*> frame_queue; // shared queue data structure for use by all threads
double* temp_frame = (double*)malloc(FRAME_LENGTH * sizeof(double)); // shared temp frame for use by the estimator and transformer threads
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

extern double* generate_frame_vector(int l);
extern double* compression(double* frame, int length);

void* transformer(void *arg)
{
    // Keep getting frames until generate_frame_vector returns NULL
    while ((temp_frame = generate_frame_vector(FRAME_LENGTH)) != NULL) {
        pthread_mutex_lock(&queue_mutex);
        frame_queue.push(temp_frame);
        pthread_mutex_unlock(&queue_mutex);
    }
    
    if (frame_queue.empty()) {
        pthread_exit(NULL);
    } else {
        temp_frame = frame_queue.front();  
        // Run compression
        temp_frame = compression(temp_frame, FRAME_LENGTH);
        printf("sleeping....\n");
        sleep(3);        /* number of seconds for sleeping*/
        printf("i'm back\n");
        for (int i = 0; i < FRAME_LENGTH; i++) {
            printf("%f\n", temp_frame[i]);
        }
        pthread_exit(NULL);
    }
}

int main(int argc, char *argv[]) {
    pthread_t transformer_thread;
    int rc;
    
    sem_init(&transformer_sem, 0, 0);

    rc = pthread_create(&transformer_thread, NULL, transformer, NULL);
    if (rc) { 
        printf("Error when creating the transformer thread!\n");
        exit(-1);
    }
    
    // Wait for the transformer thread to complete
    rc = pthread_join(transformer_thread, NULL);
    if (rc) {
        printf("Error when joining the transformer thread!\n");
        exit(-1);
    }
    
    sem_destroy(&transformer_sem);
    
    return 0;
}

