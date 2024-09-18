#pragma once

// What should be called to shutdown when raft_server is in abnormal state
#ifdef USE_PTHREAD_EXIT
extern "C" {
#include <pthread.h>
}
#define _sys_exit(status) ::pthread_exit(nullptr)

#else

#include <cstdlib>
#define _sys_exit(status) ::exit((status))

#endif
