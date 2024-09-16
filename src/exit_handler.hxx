#pragma once

// What should be called to shutdown when raft_server is in abnormal state
#ifdef USE_PTHREAD_EXIT

#define _sys_exit(status) ::pthread_exit(nullptr)

#else

#define _sys_exit(status) ::exit((status))

#endif
