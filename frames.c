#include <stddef.h>
#include "frames.h"


void queue_init(FrameQueue* q)
{
    q->head = 0;
    q->tail = 0;
    q->count = 0;
}

int queue_is_empty(FrameQueue* q)
{
    return q->count == 0;
}

int queue_is_full(FrameQueue* q)
{
    return q->count == CACHE_CAPACITY;
}

int queue_enqueue(FrameQueue* q, double* frame)
{
    if (queue_is_full(q)) return -1;
    q->frames[q->tail] = frame;
    q->tail = (q->tail + 1) % CACHE_CAPACITY;
    q->count++;
    return 0;
}

double* queue_dequeue(FrameQueue* q)
{
    if (queue_is_empty(q)) return NULL;
    double* frame = q->frames[q->head];
    q->head = (q->head + 1) % CACHE_CAPACITY;
    q->count--;
    return frame;
}

double* queue_peek(FrameQueue* q)
{
    if (queue_is_empty(q)) return NULL;
    return q->frames[q->head];
}
