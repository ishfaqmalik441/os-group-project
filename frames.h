#ifndef FRAME_QUEUE_H
#define FRAME_QUEUE_H

#define CACHE_CAPACITY 3

typedef struct {
    double* frames[CACHE_CAPACITY];
    int head;
    int tail;
    int count;
} FrameQueue;

extern FrameQueue cache_queue;

void queue_init(FrameQueue* q);
int  queue_is_empty(FrameQueue* q);
int  queue_is_full(FrameQueue* q);
int  queue_enqueue(FrameQueue* q, double* frame);
double* queue_dequeue(FrameQueue* q);
double* queue_peek(FrameQueue* q);

#endif // FRAME_QUEUE_H
