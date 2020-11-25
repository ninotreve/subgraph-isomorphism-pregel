//Acknowledgements: this code is implemented based on pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef TIME_H
#define TIME_H

#include <sys/time.h>
#include <stdio.h>

#define StartTimer(i) start_timer((i))
#define StopTimer(i) stop_timer((i))
#define ResetTimer(i) reset_timer((i))
#define InitTimer(i) init_timer((i))
#define PrintTimer(str, i)              \
    if (get_worker_id() == MASTER_RANK) \
        printf("%s : %f seconds\n", (str), get_timer((i)));

double get_current_time()
{
    timeval t;
    gettimeofday(&t, 0);
    return (double)t.tv_sec + (double)t.tv_usec / 1000000;
}

const int N_Timers = 12;
static double _timers[N_Timers]; // timers
static double _acc_time[N_Timers]; // accumulated time

void init_timers()
{
    for (int i = 0; i < N_Timers; i++) {
        _acc_time[i] = 0;
    }
}

enum TIMERS {
    // Timers for stage
    TOTAL_TIMER = 0,
    STAGE_TIMER = 1,
    COMPUTE_TIMER = 2,

    // Timers inside MATCH
    WORKER_TIMER = 3,
    SUPERSTEP_TIMER = 4,
    ACTIVE_COMPUTE_TIMER = 5,
    SYNC_MESSAGE_TIMER = 6,
    SYNC_TIMER = 7,
    AGG_TIMER = 8,

    // Timers for COMMUNICATION
    COMMUNICATION_TIMER = 11,
    SERIALIZATION_TIMER = 12,
    TRANSFER_TIMER = 13
};

void start_timer(int i)
{
    _timers[i] = get_current_time();
}

void reset_timer(int i)
{
    _timers[i] = get_current_time();
    _acc_time[i] = 0;
}

void stop_timer(int i)
{
    double t = get_current_time();
    _acc_time[i] += t - _timers[i];
}

void init_timer(int i)
{
    _acc_time[i] = 0;
}

double get_timer(int i)
{
    return _acc_time[i];
}

#endif
