/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include "nuraft.hxx"
#include "logger.h"

using namespace nuraft;

/**
 * Example implementation of Raft logger, on top of SimpleLogger.
 */
class logger_wrapper : public logger {
public:
    logger_wrapper(const std::string& log_file, int log_level = 6) {
        my_log_ = new SimpleLogger(log_file, 1024, 32*1024*1024, 10);
        my_log_->setLogLevel(log_level);
        my_log_->setDispLevel(-1);
        my_log_->setCrashDumpPath("./", true);
        my_log_->start();
    }

    ~logger_wrapper() {
        destroy();
    }

    void destroy() {
        if (my_log_) {
            my_log_->flushAll();
            my_log_->stop();
            delete my_log_;
            my_log_ = nullptr;
        }
    }

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& msg)
    {
        if (my_log_) {
            my_log_->put( level, source_file, func_name, line_number,
                          "%s", msg.c_str() );
        }
    }

    void set_level(int l) {
        if (!my_log_) return;

        if (l < 0) l = 1;
        if (l > 6) l = 6;
        my_log_->setLogLevel(l);
    }

    int get_level() {
        if (!my_log_) return 0;
        return my_log_->getLogLevel();
    }

    SimpleLogger* getLogger() const { return my_log_; }

private:
    SimpleLogger* my_log_;
};


