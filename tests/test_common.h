/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright 2017 Jung-Sang Ahn
See URL: https://github.com/greensky00/testsuite
         (v0.1.67)

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

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <functional>
#include <iostream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#if defined(__linux__) || defined(__APPLE__)
    #include <sys/stat.h>
    #include <sys/types.h>
#elif defined(WIN32) || defined(_WIN32)
    #define NOMINMAX
    #include <direct.h>
    #include <Windows.h>
    typedef SSIZE_T ssize_t;
#endif

#ifndef _CLM_DEFINED
#define _CLM_DEFINED (1)

#ifdef TESTSUITE_NO_COLOR
    #define _CLM_D_GRAY     ""
    #define _CLM_GREEN      ""
    #define _CLM_B_GREEN    ""
    #define _CLM_RED        ""
    #define _CLM_B_RED      ""
    #define _CLM_BROWN      ""
    #define _CLM_B_BROWN    ""
    #define _CLM_BLUE       ""
    #define _CLM_B_BLUE     ""
    #define _CLM_MAGENTA    ""
    #define _CLM_B_MAGENTA  ""
    #define _CLM_CYAN       ""
    #define _CLM_END        ""

    #define _CLM_WHITE_FG_RED_BG    ""
#else
    #define _CLM_D_GRAY     "\033[1;30m"
    #define _CLM_GREEN      "\033[32m"
    #define _CLM_B_GREEN    "\033[1;32m"
    #define _CLM_RED        "\033[31m"
    #define _CLM_B_RED      "\033[1;31m"
    #define _CLM_BROWN      "\033[33m"
    #define _CLM_B_BROWN    "\033[1;33m"
    #define _CLM_BLUE       "\033[34m"
    #define _CLM_B_BLUE     "\033[1;34m"
    #define _CLM_MAGENTA    "\033[35m"
    #define _CLM_B_MAGENTA  "\033[1;35m"
    #define _CLM_CYAN       "\033[36m"
    #define _CLM_B_GREY     "\033[1;37m"
    #define _CLM_END        "\033[0m"

    #define _CLM_WHITE_FG_RED_BG    "\033[37;41m"
#endif

#define _CL_D_GRAY(str)     _CLM_D_GRAY     str _CLM_END
#define _CL_GREEN(str)      _CLM_GREEN      str _CLM_END
#define _CL_RED(str)        _CLM_RED        str _CLM_END
#define _CL_B_RED(str)      _CLM_B_RED      str _CLM_END
#define _CL_MAGENTA(str)    _CLM_MAGENTA    str _CLM_END
#define _CL_BROWN(str)      _CLM_BROWN      str _CLM_END
#define _CL_B_BROWN(str)    _CLM_B_BROWN    str _CLM_END
#define _CL_B_BLUE(str)     _CLM_B_BLUE     str _CLM_END
#define _CL_B_MAGENTA(str)  _CLM_B_MAGENTA  str _CLM_END
#define _CL_CYAN(str)       _CLM_CYAN       str _CLM_END
#define _CL_B_GRAY(str)     _CLM_B_GREY     str _CLM_END

#define _CL_WHITE_FG_RED_BG(str)    _CLM_WHITE_FG_RED_BG    str _CLM_END

#endif

#define __COUT_STACK_INFO__                                                     \
       std::endl                                                                \
    << "        time: " << _CLM_D_GRAY <<                                       \
       TestSuite::getTimeString() << _CLM_END << "\n"                           \
    << "      thread: " << _CLM_BROWN                                           \
    << std::hex << std::setw(4) << std::setfill('0') <<                         \
       (std::hash<std::thread::id>{}( std::this_thread::get_id() ) & 0xffff)    \
    << std::dec << _CLM_END << "\n"                                             \
    << "          in: " << _CLM_CYAN << __func__ << "()" _CLM_END << "\n"       \
    << "          at: " << _CLM_GREEN << __FILE__ << _CLM_END ":"               \
    << _CLM_B_MAGENTA << __LINE__ << _CLM_END << "\n"                           \

// exp_value == value
#define CHK_EQ(exp_value, value)                                        \
{                                                                       \
    auto _ev = (exp_value);                                             \
    decltype(_ev) _v = (decltype(_ev))(value);                          \
    if (_ev != _v) {                                                    \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << _ev << _CLM_END "\n"        \
        << "      actual: " _CLM_B_RED << _v << _CLM_END "\n";          \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }                                                                   \
}

// exp_value != value
#define CHK_NEQ(exp_value, value)                                       \
{                                                                       \
    auto _ev = (exp_value);                                             \
    decltype(_ev) _v = (decltype(_ev))(value);                          \
    if (_ev == _v) {                                                    \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: not " _CLM_B_GREEN << _ev << _CLM_END "\n"    \
        << "      actual: " _CLM_B_RED << _v << _CLM_END "\n";          \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }                                                                   \
}

// value == true
#define CHK_OK(value)                                                   \
    if (!(value)) {                                                     \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << "true" << _CLM_END "\n"     \
        << "      actual: " _CLM_B_RED << "false" << _CLM_END "\n";     \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }

#define CHK_TRUE(value) CHK_OK(value)

// value == false
#define CHK_NOT(value)                                                  \
    if (value) {                                                        \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << "false" << _CLM_END "\n"    \
        << "      actual: " _CLM_B_RED << "true" << _CLM_END "\n";      \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }

#define CHK_FALSE(value) CHK_NOT(value)

// value == NULL
#define CHK_NULL(value)                                                 \
{                                                                       \
    auto _v = (value);                                                  \
    if (_v) {                                                           \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << "NULL" << _CLM_END "\n";    \
        printf("      actual: " _CLM_B_RED "%p" _CLM_END "\n", _v);     \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }                                                                   \
}

// value != NULL
#define CHK_NONNULL(value)                                              \
    if (!(value)) {                                                     \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << "non-NULL" << _CLM_END "\n" \
        << "      actual: " _CLM_B_RED << "NULL" << _CLM_END "\n";      \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }

// value == 0
#define CHK_Z(value)                                                    \
{                                                                       \
    auto _v = (value);                                                  \
    if ((0) != _v) {                                                    \
        std::cout                                                       \
        << __COUT_STACK_INFO__                                          \
        << "    value of: " _CLM_B_BLUE #value _CLM_END "\n"            \
        << "    expected: " _CLM_B_GREEN << "0" << _CLM_END "\n"        \
        << "      actual: " _CLM_B_RED << _v << _CLM_END "\n";          \
        TestSuite::failHandler();                                       \
        return -1;                                                      \
    }                                                                   \
}

// smaller < greater
#define CHK_SM(smaller, greater)                                \
{                                                               \
    auto _sm = (smaller);                                       \
    decltype(_sm) _gt = (decltype(_sm))(greater);               \
    if (!(_sm < _gt)) {                                         \
        std::cout                                               \
        << __COUT_STACK_INFO__                                  \
        << "    expected: "                                     \
        << _CLM_B_BLUE #smaller " < " #greater _CLM_END "\n"    \
        << "    value of "                                      \
        << _CLM_B_GREEN #smaller _CLM_END ": "                  \
        << _CLM_B_RED << _sm << _CLM_END "\n"                   \
        << "    value of "                                      \
        << _CLM_B_GREEN #greater _CLM_END ": "                  \
        << _CLM_B_RED << _gt << _CLM_END "\n";                  \
        TestSuite::failHandler();                               \
        return -1;                                              \
    }                                                           \
}

// smaller <= greater
#define CHK_SMEQ(smaller , greater)                             \
{                                                               \
    auto _sm = (smaller);                                       \
    decltype(_sm) _gt = (decltype(_sm))(greater);               \
    if (!(_sm <= _gt)) {                                        \
        std::cout                                               \
        << __COUT_STACK_INFO__                                  \
        << "    expected: "                                     \
        << _CLM_B_BLUE #smaller " <= " #greater _CLM_END "\n"   \
        << "    value of "                                      \
        << _CLM_B_GREEN #smaller _CLM_END ": "                  \
        << _CLM_B_RED << _sm << _CLM_END "\n"                   \
        << "    value of "                                      \
        << _CLM_B_GREEN #greater _CLM_END ": "                  \
        << _CLM_B_RED << _gt << _CLM_END "\n";                  \
        TestSuite::failHandler();                               \
        return -1;                                              \
    }                                                           \
}

// greater > smaller
#define CHK_GT(greater, smaller)                                \
{                                                               \
    auto _sm = (smaller);                                       \
    decltype(_sm) _gt = (decltype(_sm))(greater);               \
    if (!(_gt > _sm)) {                                         \
        std::cout                                               \
        << __COUT_STACK_INFO__                                  \
        << "    expected: "                                     \
        << _CLM_B_BLUE #greater " > " #smaller _CLM_END "\n"    \
        << "    value of "                                      \
        << _CLM_B_GREEN #greater _CLM_END ": "                  \
        << _CLM_B_RED << _gt << _CLM_END "\n"                   \
        << "    value of "                                      \
        << _CLM_B_GREEN #smaller _CLM_END ": "                  \
        << _CLM_B_RED << _sm << _CLM_END "\n";                  \
        TestSuite::failHandler();                               \
        return -1;                                              \
    }                                                           \
}

// greater >= smaller
#define CHK_GTEQ(greater, smaller)                              \
{                                                               \
    auto _sm = (smaller);                                       \
    decltype(_sm) _gt = (decltype(_sm))(greater);               \
    if (!(_gt >= _sm)) {                                        \
        std::cout                                               \
        << __COUT_STACK_INFO__                                  \
        << "    expected: "                                     \
        << _CLM_B_BLUE #greater " >= " #smaller _CLM_END "\n"   \
        << "    value of "                                      \
        << _CLM_B_GREEN #greater _CLM_END ": "                  \
        << _CLM_B_RED << _gt << _CLM_END "\n"                   \
        << "    value of "                                      \
        << _CLM_B_GREEN #smaller _CLM_END ": "                  \
        << _CLM_B_RED << _sm << _CLM_END "\n";                  \
        TestSuite::failHandler();                               \
        return -1;                                              \
    }                                                           \
}


using test_func = std::function<int()>;

class TestArgsBase;
using test_func_args = std::function<int(TestArgsBase*)>;

class TestSuite;
class TestArgsBase {
public:
    virtual ~TestArgsBase() { }
    void setCallback(std::string test_name,
                     test_func_args func,
                     TestSuite* test_instance) {
        testName = test_name;
        testFunction = func;
        testInstance = test_instance;
    }
    void testAll() { testAllInternal(0); }
    virtual void setParam(size_t param_no, size_t param_idx) = 0;
    virtual size_t getNumSteps(size_t param_no) = 0;
    virtual size_t getNumParams() = 0;
    virtual std::string toString() = 0;

private:
    inline void testAllInternal(size_t depth);
    std::string testName;
    test_func_args testFunction;
    TestSuite* testInstance;
};

class TestArgsWrapper {
public:
    TestArgsWrapper(TestArgsBase* _test_args) : test_args(_test_args) {}
    ~TestArgsWrapper() { delete test_args; }
    TestArgsBase* getArgs() const { return test_args; }
    operator TestArgsBase*() const { return getArgs(); }
private:
    TestArgsBase* test_args;
};

enum class StepType {
    LINEAR,
    EXPONENTIAL
};

template<typename T>
class TestRange {
public:
    TestRange() : type(RangeType::NONE), begin(), end(), step() {}

    // Constructor for given values
    TestRange(const std::vector<T>& _array)
        : type(RangeType::ARRAY), array(_array)
        , begin(), end(), step() {}

    // Constructor for regular steps
    TestRange(T _begin, T _end, T _step, StepType _type)
        : begin(_begin), end(_end), step(_step)
    {
        if (_type == StepType::LINEAR) {
            type = RangeType::LINEAR;
        } else {
            type = RangeType::EXPONENTIAL;
        }
    }

    T getEntry(size_t idx) {
        if (type == RangeType::ARRAY) {
            return (T)(array[idx]);
        } else if (type == RangeType::LINEAR) {
            return (T)(begin + step * idx);
        } else if (type == RangeType::EXPONENTIAL) {
            ssize_t _begin = begin;
            ssize_t _step = step;
            ssize_t _ret = (ssize_t)( _begin * std::pow(_step, idx) );
            return (T)(_ret);
        }

        return begin;
    }

    size_t getSteps() {
        if (type == RangeType::ARRAY) {
            return array.size();
        } else if (type == RangeType::LINEAR) {
            return ((end - begin) / step) + 1;
        } else if (type == RangeType::EXPONENTIAL) {
            ssize_t coe = ((ssize_t)end) / ((ssize_t)begin);
            double steps_double = (double)std::log(coe) / std::log(step);
            return (size_t)(steps_double + 1);
        }

        return 0;
    }

private:
    enum class RangeType {
        NONE,
        ARRAY,
        LINEAR,
        EXPONENTIAL
    };

    RangeType type;
    std::vector<T> array;
    T begin;
    T end;
    T step;
};

struct TestOptions {
    TestOptions()
        : printTestMessage(false)
        , abortOnFailure(false)
        , preserveTestFiles(false)
        {}
    bool printTestMessage;
    bool abortOnFailure;
    bool preserveTestFiles;
};

class TestSuite {
    friend TestArgsBase;
private:
    static std::mutex& getResMsgLock() {
        static std::mutex res_msg_lock;
        return res_msg_lock;
    }
    static std::string& getResMsg() {
        static std::string res_msg;
        return res_msg;
    }
    static std::string& getInfoMsg() {
        thread_local std::string info_msg;
        return info_msg;
    }
    static std::string& getTestName() {
        static std::string test_name;
        return test_name;
    }
    static TestSuite*& getCurTest() {
        static TestSuite* cur_test;
        return cur_test;
    }
public:
    static bool& globalMsgFlag() {
        static bool global_msg_flag = false;
        return global_msg_flag;
    }
    static std::string getCurrentTestName() {
        return getTestName();
    }
    static bool isMsgAllowed() {
        TestSuite* cur_test = TestSuite::getCurTest();
        if ( cur_test &&
             (cur_test->options.printTestMessage || cur_test->displayMsg) &&
             !cur_test->suppressMsg ) {
            return true;
        }
        if (globalMsgFlag()) return true;
        return false;
    }

    static void setInfo(const char* format, ...) {
        thread_local char info_buf[4096];
        size_t len = 0;
        va_list args;
        va_start(args, format);
        len += vsnprintf(info_buf + len, 4096 - len, format, args);
        va_end(args);
        getInfoMsg() = info_buf;
    }
    static void clearInfo() {
        getInfoMsg().clear();
    }

    static void failHandler() {
        if (!getInfoMsg().empty()) {
            std::cout << "        info: " << getInfoMsg() << std::endl;
        }
    }

    static void usage(int argc, char** argv) {
        printf("\n");
        printf("Usage: %s [-f <keyword>] [-r <parameter>] [-p]\n", argv[0]);
        printf("\n");
        printf("    -f, --filter\n");
        printf("        Run specific tests matching the given keyword.\n");
        printf("    -r, --range\n");
        printf("        Run TestRange-based tests using given parameter value.\n");
        printf("    -p, --preserve\n");
        printf("        Do not clean up test files.\n");
        printf("    --abort-on-failure\n");
        printf("        Immediately abort the test if failure happens.\n");
        printf("    --suppress-msg\n");
        printf("        Suppress test messages.\n");
        printf("    --display-msg\n");
        printf("        Display test messages.\n");
        printf("\n");
    }

    static std::string usToString(uint64_t us) {
        std::stringstream ss;
        if (us < 1000) {
            // us
            ss << std::fixed << std::setprecision(0) << us << " us";
        } else if (us < 1000000) {
            // ms
            double tmp = static_cast<double>(us / 1000.0);
            ss << std::fixed << std::setprecision(1) << tmp << " ms";
        } else if (us < (uint64_t)600 * 1000000) {
            // second: 1 s -- 600 s (10 mins)
            double tmp = static_cast<double>(us / 1000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << " s";
        } else {
            // minute
            double tmp = static_cast<double>(us / 60.0 / 1000000.0);
            ss << std::fixed << std::setprecision(0) << tmp << " m";
        }
        return ss.str();
    }

    static std::string countToString(uint64_t count) {
        std::stringstream ss;
        if (count < 1000) {
            ss << count;
        } else if (count < 1000000) {
            // K
            double tmp = static_cast<double>(count / 1000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "K";
        } else if (count < (uint64_t)1000000000) {
            // M
            double tmp = static_cast<double>(count / 1000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "M";
        } else {
            // B
            double tmp = static_cast<double>(count / 1000000000.0);
            ss << std::fixed << std::setprecision(1) << tmp << "B";
        }
        return ss.str();
    }

    static std::string sizeToString(uint64_t size) {
        std::stringstream ss;
        if (size < 1024) {
            ss << size << " B";
        } else if (size < 1024*1024) {
            // K
            double tmp = static_cast<double>(size / 1024.0);
            ss << std::fixed << std::setprecision(1) << tmp << " KiB";
        } else if (size < (uint64_t)1024*1024*1024) {
            // M
            double tmp = static_cast<double>(size / 1024.0 / 1024.0);
            ss << std::fixed << std::setprecision(1) << tmp << " MiB";
        } else {
            // B
            double tmp = static_cast<double>(size / 1024.0 / 1024.0 / 1024.0);
            ss << std::fixed << std::setprecision(1) << tmp << " GiB";
        }
        return ss.str();
    }

private:
    struct TimeInfo {
        TimeInfo(std::tm* src)
            : year(src->tm_year + 1900)
            , month(src->tm_mon + 1)
            , day(src->tm_mday)
            , hour(src->tm_hour)
            , min(src->tm_min)
            , sec(src->tm_sec)
            , msec(0)
            , usec(0) {}
        TimeInfo(std::chrono::system_clock::time_point now) {
            std::time_t raw_time = std::chrono::system_clock::to_time_t(now);
            std::tm* lt_tm = std::localtime(&raw_time);
            year = lt_tm->tm_year + 1900;
            month = lt_tm->tm_mon + 1;
            day = lt_tm->tm_mday;
            hour = lt_tm->tm_hour;
            min = lt_tm->tm_min;
            sec = lt_tm->tm_sec;

            size_t us_epoch = std::chrono::duration_cast
                              < std::chrono::microseconds >
                              ( now.time_since_epoch() ).count();
            msec = (us_epoch / 1000) % 1000;
            usec = us_epoch % 1000;
        }
        int year;
        int month;
        int day;
        int hour;
        int min;
        int sec;
        int msec;
        int usec;
    };

public:
    TestSuite(int argc = 0, char **argv = nullptr)
        : cntPass(0)
        , cntFail(0)
        , useGivenRange(false)
        , preserveTestFiles(false)
        , forceAbortOnFailure(false)
        , suppressMsg(false)
        , displayMsg(false)
        , givenRange(0)
        , startTimeGlobal(std::chrono::system_clock::now())
    {
        for (int ii=1; ii<argc; ++ii) {
            // Filter.
            if ( ii < argc-1 &&
                 (!strcmp(argv[ii], "-f") ||
                  !strcmp(argv[ii], "--filter")) ) {
                filter = argv[++ii];
            }

            // Range.
            if ( ii < argc-1 &&
                 (!strcmp(argv[ii], "-r") ||
                  !strcmp(argv[ii], "--range")) ) {
                givenRange = atoi(argv[++ii]);
                useGivenRange = true;
            }

            // Do not clean up test files after test.
            if ( !strcmp(argv[ii], "-p") ||
                 !strcmp(argv[ii], "--preserve") ) {
                preserveTestFiles = true;
            }

            // Force abort on failure.
            if ( !strcmp(argv[ii], "--abort-on-failure") ) {
                forceAbortOnFailure = true;
            }

            // Suppress test messages.
            if ( !strcmp(argv[ii], "--suppress-msg") ) {
                suppressMsg = true;
            }

            // Display test messages.
            if ( !strcmp(argv[ii], "--display-msg") && !suppressMsg ) {
                displayMsg = true;
            }

            // Help
            if ( !strcmp(argv[ii], "-h") ||
                 !strcmp(argv[ii], "--help") ) {
                usage(argc, argv);
                exit(0);
            }
        }
    }

    ~TestSuite() {
        std::chrono::time_point<std::chrono::system_clock> cur_time =
                std::chrono::system_clock::now();;
        std::chrono::duration<double> elapsed = cur_time - startTimeGlobal;
        std::string time_str = usToString
                               ( (uint64_t)(elapsed.count() * 1000000) );

        printf(_CL_GREEN("%zu") " tests passed", cntPass);
        if (cntFail) {
            printf(", " _CL_RED("%zu") " tests failed", cntFail);
        }
        printf(" out of " _CL_CYAN("%zu") " (" _CL_BROWN("%s") ")\n",
               cntPass+cntFail, time_str.c_str());
    }

    // === Helper functions ====================================
    static std::string getTestFileName(const std::string& prefix) {
        TimeInfo lt(std::chrono::system_clock::now());
        (void)lt;

        char time_char[64];
        sprintf(time_char, "%04d%02d%02d_%02d%02d%02d",
                lt.year, lt.month, lt.day, lt.hour, lt.min, lt.sec);

        std::string ret = prefix;
        ret += "_";
        ret += time_char;
        return ret;
    }

    static std::string getTimeString() {
        TimeInfo lt(std::chrono::system_clock::now());
        char time_char[64];
        sprintf(time_char, "%04d-%02d-%02d %02d:%02d:%02d.%03d%03d",
                lt.year, lt.month, lt.day, lt.hour, lt.min, lt.sec, lt.msec, lt.usec);
        return time_char;
    }
    static std::string getTimeStringShort() {
        TimeInfo lt(std::chrono::system_clock::now());
        char time_char[64];
        sprintf(time_char, "%02d:%02d.%03d %03d",
                lt.min, lt.sec, lt.msec, lt.usec);
        return time_char;
    }
    static std::string getTimeStringPlain() {
        TimeInfo lt(std::chrono::system_clock::now());
        char time_char[64];
        sprintf(time_char, "%02d%02d_%02d%02d%02d",
                lt.month, lt.day, lt.hour, lt.min, lt.sec);
        return time_char;
    }

    static int mkdir(const std::string& path) {
#if defined(__linux__) || defined(__APPLE__)
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            return ::mkdir(path.c_str(), 0755);
        }

#elif defined(WIN32) || defined(_WIN32)
        if (GetFileAttributes(path.c_str()) == INVALID_FILE_ATTRIBUTES) {
            return _mkdir(path.c_str());
        }
#endif
        return 0;
    }
    static int copyfile(const std::string& src,
                        const std::string& dst) {
#if defined(__linux__) || defined(__APPLE__)
        std::string cmd = "cp -R " + src + " " + dst;
        int rc = ::system(cmd.c_str());
        return rc;

#elif defined(WIN32) || defined(_WIN32)
        // TODO: `xcopy` only copies folders, not files.
        std::string cmd = "xcopy /e /i /h " + src + " " + dst + " > NUL";
        int rc = ::system(cmd.c_str());
        return rc;
#endif
    }
    static int remove(const std::string& path) {
        int rc = ::remove(path.c_str());
        return rc;
    }
    static bool exist(const std::string& path) {
#if defined(__linux__) || defined(__APPLE__)
        struct stat st;
        int result = stat(path.c_str(), &st);
        return (result == 0);

#elif defined(WIN32) || defined(_WIN32)
        if (GetFileAttributes(path.c_str()) != INVALID_FILE_ATTRIBUTES) {
            return true;
        }
        return false;
#endif
    }

    enum TestPosition {
        BEGINNING_OF_TEST   = 0,
        MIDDLE_OF_TEST      = 1,
        END_OF_TEST         = 2,
    };
    static void clearTestFile( const std::string& prefix,
                               TestPosition test_pos = MIDDLE_OF_TEST ) {
        TestSuite*& cur_test = TestSuite::getCurTest();
        if ( test_pos == END_OF_TEST &&
             ( cur_test->preserveTestFiles ||
               cur_test->options.preserveTestFiles ) ) return;

        int r;
#if defined(__linux__) || defined(__APPLE__)
        std::string command = "rm -rf ";
        command += prefix;
        command += "*";
        r = system(command.c_str());
        (void)r;

#elif defined(WIN32) || defined(_WIN32)
        std::string command = "del /s /f /q ";
        command += prefix;
        command += "* > NUL";
        r = system(command.c_str());
        (void)r;

        // Windows `del` operation cannot delete folders.
        // Just in case if there are any folders.
        WIN32_FIND_DATA filedata;
        HANDLE hfind;
        std::string query_str = prefix + "*";
        hfind = FindFirstFile(query_str.c_str(), &filedata);
        while (hfind != INVALID_HANDLE_VALUE) {
            std::string f_name(filedata.cFileName);
            size_t f_name_pos = f_name.find(prefix);
            if (f_name_pos != std::string::npos) {
                command = "rmdir /s /q " + f_name + " > NUL";
                r = system(command.c_str());
                (void)r;
            }

            if (!FindNextFile(hfind, &filedata)) {
                FindClose(hfind);
                hfind = INVALID_HANDLE_VALUE;
            }
        }
#endif
    }

    static void setResultMessage(const std::string& msg) {
        TestSuite::getResMsg() = msg;
    }

    static void appendResultMessage(const std::string& msg) {
        std::lock_guard<std::mutex> l(TestSuite::getResMsgLock());
        TestSuite::getResMsg() += msg;
    }

    static size_t _msg(const char* format, ...) {
        size_t cur_len = 0;
        TestSuite* cur_test = TestSuite::getCurTest();
        if ( ( cur_test &&
               (cur_test->options.printTestMessage || cur_test->displayMsg) &&
               !cur_test->suppressMsg ) ||
             globalMsgFlag() ) {
            va_list args;
            va_start(args, format);
            cur_len += vprintf(format, args);
            va_end(args);
        }
        return cur_len;
    }
    static size_t _msgt(const char* format, ...) {
        size_t cur_len = 0;
        TestSuite* cur_test = TestSuite::getCurTest();
        if ( ( cur_test &&
               (cur_test->options.printTestMessage || cur_test->displayMsg) &&
               !cur_test->suppressMsg ) ||
             globalMsgFlag() ) {
            std::cout << _CLM_D_GRAY
                      << getTimeStringShort() << _CLM_END << "] ";
            va_list args;
            va_start(args, format);
            cur_len += vprintf(format, args);
            va_end(args);
        }
        return cur_len;
    }

    class Msg {
    public:
        Msg() {}

        template<typename T>
        inline Msg& operator<<(const T& data) {
            if (TestSuite::isMsgAllowed()) {
                std::cout << data;
            }
            return *this;
        }

        using MyCout = std::basic_ostream< char, std::char_traits<char> >;
        typedef MyCout& (*EndlFunc)(MyCout&);

        Msg& operator<<(EndlFunc func) {
            if (TestSuite::isMsgAllowed()) {
                func(std::cout);
            }
            return *this;
        }
    };

    static void sleep_us(size_t us, const std::string& msg = std::string()) {
        if (!msg.empty()) TestSuite::_msg("%s (%zu us)\n", msg.c_str(), us);
        std::this_thread::sleep_for(std::chrono::microseconds(us));
    }
    static void sleep_ms(size_t ms, const std::string& msg = std::string()) {
        if (!msg.empty()) TestSuite::_msg("%s (%zu ms)\n", msg.c_str(), ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    static void sleep_sec(size_t sec, const std::string& msg = std::string()) {
        if (!msg.empty()) TestSuite::_msg("%s (%zu s)\n", msg.c_str(), sec);
        std::this_thread::sleep_for(std::chrono::seconds(sec));
    }
    static std::string lzStr(size_t digit, uint64_t num) {
        std::stringstream ss;
        ss << std::setw(digit) << std::setfill('0') << std::to_string(num);
        return ss.str();
    }
    static double calcThroughput(uint64_t ops, uint64_t elapsed_us) {
        return ops * 1000000.0 / elapsed_us;
    }
    static std::string throughputStr(uint64_t ops, uint64_t elapsed_us) {
        return countToString(ops * 1000000 / elapsed_us);
    }
    static std::string sizeThroughputStr(uint64_t size_byte, uint64_t elapsed_us) {
        return sizeToString(size_byte * 1000000 / elapsed_us);
    }

    // === Timer things ====================================
    class Timer {
    public:
        Timer() : duration_ms(0) {
            reset();
        }
        Timer(size_t _duration_ms) : duration_ms(_duration_ms) {
            reset();
        }
        inline bool timeout() { return timeover(); }
        bool timeover() {
            auto cur = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed = cur - start;
            if (duration_ms < elapsed.count() * 1000) return true;
            return false;
        }
        uint64_t getTimeSec() {
            auto cur = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed = cur - start;
            return (uint64_t)(elapsed.count());
        }
        uint64_t getTimeMs() {
            auto cur = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed = cur - start;
            return (uint64_t)(elapsed.count() * 1000);
        }
        uint64_t getTimeUs() {
            auto cur = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed = cur - start;
            return (uint64_t)(elapsed.count() * 1000000);
        }
        void reset() {
            start = std::chrono::system_clock::now();
        }
        void resetSec(size_t _duration_sec) {
            duration_ms = _duration_sec * 1000;
            reset();
        }
        void resetMs(size_t _duration_ms) {
            duration_ms = _duration_ms;
            reset();
        }
    private:
        std::chrono::time_point<std::chrono::system_clock> start;
        size_t duration_ms;
    };

    // === Workload generator things ====================================
    class WorkloadGenerator {
    public:
        WorkloadGenerator(double ops_per_sec = 0.0, uint64_t max_ops_per_batch = 0)
            : opsPerSec(ops_per_sec)
            , maxOpsPerBatch(max_ops_per_batch)
            , numOpsDone(0) {
            reset();
        }
        void reset() {
            start = std::chrono::system_clock::now();
            numOpsDone = 0;
        }
        size_t getNumOpsToDo() {
            if (opsPerSec <= 0) return 0;

            auto cur = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed = cur - start;

            double exp = opsPerSec * elapsed.count();
            if (numOpsDone < exp) {
                if (maxOpsPerBatch) {
                    return std::min(maxOpsPerBatch, (uint64_t)exp - numOpsDone);
                }
                return (uint64_t)exp - numOpsDone;
            }
            return 0;
        }
        void addNumOpsDone(size_t num) {
            numOpsDone += num;
        }
    private:
        std::chrono::time_point<std::chrono::system_clock> start;
        double opsPerSec;
        uint64_t maxOpsPerBatch;
        uint64_t numOpsDone;
    };

    // === Progress things ==================================
    // Progress that knows the maximum value.
    class Progress {
    public:
        Progress(uint64_t _num,
                 const std::string& _comment = std::string(),
                 const std::string& _unit = std::string())
            : curValue(0)
            , num(_num)
            , timer(0)
            , lastPrintTimeUs(timer.getTimeUs())
            , comment(_comment)
            , unit(_unit) {}
        void update(uint64_t cur) {
            curValue = cur;
            uint64_t curTimeUs = timer.getTimeUs();
            if (curTimeUs - lastPrintTimeUs > 50000 ||
                cur == 0 || curValue >= num) {
                // Print every 0.05 sec (20 Hz).
                lastPrintTimeUs = curTimeUs;
                std::string _comment =
                    (comment.empty()) ? "" : comment + ": ";
                std::string _unit =
                    (unit.empty()) ? "" : unit + " ";

                _msg("\r%s%ld/%ld %s(%.1f%%)",
                     _comment.c_str(), curValue, num, _unit.c_str(),
                     (double)curValue*100/num);
                fflush(stdout);
            }
            if (curValue >= num) {
                _msg("\n");
                fflush(stdout);
            }
        }
        void done() { if (curValue < num) update(num); }
    private:
        uint64_t curValue;
        uint64_t num;
        Timer timer;
        uint64_t lastPrintTimeUs;
        std::string comment;
        std::string unit;
    };

    // Progress that doesn't know the maximum value.
    class UnknownProgress {
    public:
        UnknownProgress(const std::string& _comment = std::string(),
                        const std::string& _unit = std::string())
            : curValue(0)
            , timer(0)
            , lastPrintTimeUs(timer.getTimeUs())
            , comment(_comment)
            , unit(_unit) {}
        void update(uint64_t cur) {
            curValue = cur;
            uint64_t curTimeUs = timer.getTimeUs();
            if ( curTimeUs - lastPrintTimeUs > 50000 ||
                 cur == 0 ) {
                // Print every 0.05 sec (20 Hz).
                lastPrintTimeUs = curTimeUs;
                std::string _comment =
                    (comment.empty()) ? "" : comment + ": ";
                std::string _unit =
                    (unit.empty()) ? "" : unit + " ";

                _msg("\r%s%ld %s", _comment.c_str(), curValue, _unit.c_str());
                fflush(stdout);
            }
        }
        void done() {
            _msg("\n");
            fflush(stdout);
        }
    private:
        uint64_t curValue;
        Timer timer;
        uint64_t lastPrintTimeUs;
        std::string comment;
        std::string unit;
    };

    // === Displayer things ==================================
    class Displayer {
    public:
        Displayer(size_t num_raws, size_t num_cols)
            : numRaws(num_raws)
            , numCols(num_cols)
            , colWidth(num_cols, 20)
            , context(num_raws, std::vector<std::string>(num_cols)) {}
        void init() {
            for (size_t ii=0; ii<numRaws; ++ii) _msg("\n");
        }
        void setWidth(std::vector<size_t>& src) {
            size_t num_src = src.size();
            if (!num_src) return;

            for (size_t ii=0; ii<num_src; ++ii) {
                colWidth[ii] = src[ii];
            }
            for (size_t ii=num_src; ii<numCols; ++ii) {
                colWidth[ii] = src[num_src-1];
            }
        }
        void set(size_t raw_idx, size_t col_idx, const char* format, ...) {
            if (raw_idx >= numRaws || col_idx >= numCols) return;

            thread_local char info_buf[32];
            size_t len = 0;
            va_list args;
            va_start(args, format);
            len += vsnprintf(info_buf + len, 20 - len, format, args);
            va_end(args);
            context[raw_idx][col_idx] = info_buf;
        }
        void print() {
            _msg("\033[%zuA", numRaws);
            for (size_t ii=0; ii<numRaws; ++ii) {
                std::stringstream ss;
                for (size_t jj=0; jj<numCols; ++jj) {
                    ss << std::setw(colWidth[jj]) << context[ii][jj];
                }
                _msg("\r%s\n", ss.str().c_str());
            }
        }
    private:
        size_t numRaws;
        size_t numCols;
        std::vector<size_t> colWidth;
        std::vector< std::vector< std::string > > context;
    };

    // === Gc things ====================================
    template<typename T, typename T2 = T>
    class GcVar {
    public:
        GcVar(T& _src, T2 _to)
            : src(_src), to(_to) {}
        ~GcVar() {
            // GC by value.
            src = to;
        }
    private:
        T& src;
        T2 to;
    };

    class GcFunc {
    public:
        GcFunc(std::function<void()> _func)
            : func(_func) {}
        ~GcFunc() {
            // GC by function.
            func();
        }
    private:
        std::function<void()> func;
    };

    // === Thread things ====================================
    struct ThreadArgs { /* Opaque. */ };
    using ThreadFunc = std::function< int(ThreadArgs*) >;
    using ThreadExitHandler = std::function< void(ThreadArgs*) >;

private:
    struct ThreadInternalArgs {
        ThreadInternalArgs() : userArgs(nullptr), func(nullptr), rc(0) {}
        ThreadArgs* userArgs;
        ThreadFunc func;
        int rc;
    };

public:
    struct ThreadHolder {
        ThreadHolder() : tid(nullptr), handler(nullptr) {}
        ThreadHolder(std::thread* _tid, ThreadExitHandler _handler)
            : tid(_tid), handler(_handler) {}
        ThreadHolder(ThreadArgs* u_args,
                     ThreadFunc t_func,
                     ThreadExitHandler t_handler)
            : tid(nullptr), handler(nullptr)
        { spawn(u_args, t_func, t_handler); }

        ~ThreadHolder() { join(true); }

        void spawn(ThreadArgs* u_args,
                   ThreadFunc t_func,
                   ThreadExitHandler t_handler) {
            if (tid) return;
            handler = t_handler;
            args.userArgs = u_args;
            args.func = t_func;
            tid = new std::thread(spawnThread, &args);
        }

        void join(bool force = false) {
            if (!tid) return;
            if (tid->joinable()) {
                if (force) {
                    // Force kill.
                    handler(args.userArgs);
                }
                tid->join();
            }
            delete tid;
            tid = nullptr;
        }
        int getResult() const { return args.rc; }
        std::thread* tid;
        ThreadExitHandler handler;
        ThreadInternalArgs args;
    };


    // === doTest things ====================================

    // 1) Without parameter.
    void doTest( const std::string& test_name,
                 test_func func )
    {
        if (!matchFilter(test_name)) return;

        readyTest(test_name);
        TestSuite::getResMsg() = "";
        TestSuite::getInfoMsg() = "";
        TestSuite::getCurTest() = this;
        int ret = func();
        reportTestResult(test_name, ret);
    }

    // 2) Ranged parameter.
    template<typename T, typename F>
    void doTest( std::string test_name,
                 F func,
                 TestRange<T> range )
    {
        if (!matchFilter(test_name)) return;

        size_t n = (useGivenRange) ? 1 : range.getSteps();
        size_t i;

        for (i=0; i<n; ++i) {
            std::string actual_test_name = test_name;
            std::stringstream ss;


            T cur_arg = (useGivenRange)
                        ? givenRange
                        : range.getEntry(i);

            ss << cur_arg;
            actual_test_name += " (" + ss.str() + ")";
            readyTest(actual_test_name);

            TestSuite::getResMsg() = "";
            TestSuite::getInfoMsg() = "";
            TestSuite::getCurTest() = this;

            int ret = func(cur_arg);
            reportTestResult(actual_test_name, ret);
        }
    }

    // 3) Generic one-time parameters.
    template<typename T1, typename... T2, typename F>
    void doTest( const std::string& test_name,
                 F func,
                 T1 arg1,
                 T2... args )
    {
        if (!matchFilter(test_name)) return;

        readyTest(test_name);
        TestSuite::getResMsg() = "";
        TestSuite::getInfoMsg() = "";
        TestSuite::getCurTest() = this;
        int ret = func(arg1, args...);
        reportTestResult(test_name, ret);
    }

    // 4) Multi composite parameters.
    template<typename F>
    void doTest( const std::string& test_name,
                 F func,
                 TestArgsWrapper& args_wrapper )
    {
        if (!matchFilter(test_name)) return;

        TestArgsBase* args = args_wrapper.getArgs();
        args->setCallback(test_name, func, this);
        args->testAll();
    }

    TestOptions options;

private:
    void doTestCB( const std::string& test_name,
                   test_func_args func,
                   TestArgsBase* args )
    {
        readyTest(test_name);
        TestSuite::getResMsg() = "";
        TestSuite::getInfoMsg() = "";
        TestSuite::getCurTest() = this;
        int ret = func(args);
        reportTestResult(test_name, ret);
    }

    static void spawnThread(ThreadInternalArgs* args) {
        args->rc = args->func(args->userArgs);
    }

    bool matchFilter(const std::string& test_name) {
        if (!filter.empty() &&
            test_name.find(filter) == std::string::npos) {
            // Doesn't match with the given filter.
            return false;
        }
        return true;
    }

    void readyTest(const std::string& test_name) {
        printf("[ " "...." " ] %s\n", test_name.c_str());
        if ( (options.printTestMessage || displayMsg) &&
             !suppressMsg ) {
            printf(_CL_D_GRAY("   === TEST MESSAGE (BEGIN) ===\n"));
        }
        fflush(stdout);

        getTestName() = test_name;
        startTimeLocal = std::chrono::system_clock::now();
    }

    void reportTestResult(const std::string& test_name,
                          int result)
    {
        std::chrono::time_point<std::chrono::system_clock> cur_time =
                std::chrono::system_clock::now();;
        std::chrono::duration<double> elapsed = cur_time - startTimeLocal;
        std::string time_str = usToString
                               ( (uint64_t)(elapsed.count() * 1000000) );

        char msg_buf[1024];
        std::string res_msg = TestSuite::getResMsg();
        sprintf(msg_buf, "%s (" _CL_BROWN("%s") ")%s%s",
                test_name.c_str(),
                time_str.c_str(),
                (res_msg.empty() ? "" : ": "),
                res_msg.c_str() );

        if (result < 0) {
            printf("[ " _CL_RED("FAIL") " ] %s\n", msg_buf);
            cntFail++;
        } else {
            if ( (options.printTestMessage || displayMsg) &&
                 !suppressMsg ) {
                printf(_CL_D_GRAY("   === TEST MESSAGE (END) ===\n"));
            } else {
                // Move a line up.
                printf("\033[1A");
                // Clear current line.
                printf("\r");
                // And then overwrite.
            }
            printf("[ " _CL_GREEN("PASS") " ] %s\n", msg_buf);
            cntPass++;
        }

        if ( result != 0 &&
             (options.abortOnFailure || forceAbortOnFailure) ) {
            abort();
        }
        getTestName().clear();
    }

    size_t cntPass;
    size_t cntFail;
    std::string filter;
    bool useGivenRange;
    bool preserveTestFiles;
    bool forceAbortOnFailure;
    bool suppressMsg;
    bool displayMsg;
    int64_t givenRange;
    // Start time of each test.
    std::chrono::time_point<std::chrono::system_clock> startTimeLocal;
    // Start time of the entire test suite.
    std::chrono::time_point<std::chrono::system_clock> startTimeGlobal;
};

// ===== Functor =====

struct TestArgsSetParamFunctor {
    template<typename T>
    void operator()(T* t, TestRange<T>& r, size_t param_idx) const {
        *t = r.getEntry(param_idx);
    }
};

template<std::size_t I = 0,
         typename FuncT,
         typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
TestArgsSetParamScan(int,
                     std::tuple<Tp*...> &,
                     std::tuple<TestRange<Tp>...> &,
                     FuncT,
                     size_t) { }

template<std::size_t I = 0,
         typename FuncT,
         typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
TestArgsSetParamScan(int index,
                     std::tuple<Tp*...>& t,
                     std::tuple<TestRange<Tp>...>& r,
                     FuncT f,
                     size_t param_idx) {
    if (index == 0) f(std::get<I>(t), std::get<I>(r), param_idx);
    TestArgsSetParamScan<I + 1, FuncT, Tp...>(index-1, t, r, f, param_idx);
}
struct TestArgsGetNumStepsFunctor {
    template<typename T>
    void operator()(T* t, TestRange<T>& r, size_t& steps_ret) const {
        (void)t;
        steps_ret = r.getSteps();
    }
};

template<std::size_t I = 0,
         typename FuncT,
         typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
TestArgsGetStepsScan(int,
                     std::tuple<Tp*...> &,
                     std::tuple<TestRange<Tp>...> &,
                     FuncT,
                     size_t) { }

template<std::size_t I = 0,
         typename FuncT,
         typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
TestArgsGetStepsScan(int index,
                     std::tuple<Tp*...>& t,
                     std::tuple<TestRange<Tp>...>& r,
                     FuncT f,
                     size_t& steps_ret) {
    if (index == 0) f(std::get<I>(t), std::get<I>(r), steps_ret);
    TestArgsGetStepsScan<I + 1, FuncT, Tp...>(index-1, t, r, f, steps_ret);
}

#define TEST_ARGS_CONTENTS()                               \
    void setParam(size_t param_no, size_t param_idx) {     \
        TestArgsSetParamScan(param_no, args, ranges,       \
                             TestArgsSetParamFunctor(),    \
                             param_idx); }                 \
    size_t getNumSteps(size_t param_no) {                  \
        size_t ret = 0;                                    \
        TestArgsGetStepsScan(param_no, args, ranges,       \
                             TestArgsGetNumStepsFunctor(), \
                             ret);                         \
        return ret; }                                      \
    size_t getNumParams() {                                \
        return std::tuple_size<decltype(args)>::value;     \
    }


// ===== TestArgsBase =====

void TestArgsBase::testAllInternal(size_t depth) {
    size_t i;
    size_t n_params = getNumParams();
    size_t n_steps = getNumSteps(depth);

    for (i=0; i<n_steps; ++i) {
        setParam(depth, i);
        if (depth+1 < n_params) {
            testAllInternal(depth+1);
        } else {
            std::string test_name;
            std::string args_name = toString();
            if (!args_name.empty()) {
                test_name = testName + " (" + args_name + ")";
            }
            testInstance->doTestCB(test_name,
                                   testFunction,
                                   this);
        }
    }
}

// ===== Parameter macros =====

#define DEFINE_PARAMS_2(name,                                  \
                        type1, param1, range1,                 \
                        type2, param2, range2)                 \
    class name ## _class : public TestArgsBase {               \
    public:                                                    \
        name ## _class() {                                     \
            args = std::make_tuple(&param1, &param2);          \
            ranges = std::make_tuple(                          \
                         TestRange<type1>range1,               \
                         TestRange<type2>range2 );             \
        }                                                      \
        std::string toString() {                               \
            std::stringstream ss;                              \
            ss << param1 << ", " << param2;                    \
            return ss.str();                                   \
        }                                                      \
        TEST_ARGS_CONTENTS()                                   \
        type1 param1;                                          \
        type2 param2;                                          \
    private:                                                   \
        std::tuple<type1*, type2*> args;                       \
        std::tuple<TestRange<type1>, TestRange<type2>> ranges; \
    };

#define DEFINE_PARAMS_3(name,                                  \
                        type1, param1, range1,                 \
                        type2, param2, range2,                 \
                        type3, param3, range3)                 \
    class name ## _class : public TestArgsBase {               \
    public:                                                    \
        name ## _class() {                                     \
            args = std::make_tuple(&param1, &param2, &param3); \
            ranges = std::make_tuple(                          \
                         TestRange<type1>range1,               \
                         TestRange<type2>range2,               \
                         TestRange<type3>range3 );             \
        }                                                      \
        std::string toString() {                               \
            std::stringstream ss;                              \
            ss << param1 << ", " << param2 << ", " << param3;  \
            return ss.str();                                   \
        }                                                      \
        TEST_ARGS_CONTENTS()                                   \
        type1 param1;                                          \
        type2 param2;                                          \
        type3 param3;                                          \
    private:                                                   \
        std::tuple<type1*, type2*, type3*> args;               \
        std::tuple<TestRange<type1>,                           \
                   TestRange<type2>,                           \
                   TestRange<type3>> ranges;                   \
    };

#define SET_PARAMS(name) \
    TestArgsWrapper name(new name ## _class())

#define GET_PARAMS(name) \
    name ## _class* name = static_cast<name ## _class*>(TEST_args_base__)

#define PARAM_BASE TestArgsBase* TEST_args_base__

#define TEST_SUITE_AUTO_PREFIX __func__

#define TEST_SUITE_PREPARE_PATH(path)                               \
    const std::string _ts_auto_prefiix_ = TEST_SUITE_AUTO_PREFIX;   \
    TestSuite::clearTestFile(_ts_auto_prefiix_);                    \
    path = TestSuite::getTestFileName(_ts_auto_prefiix_);

#define TEST_SUITE_CLEANUP_PATH()                       \
    TestSuite::clearTestFile( _ts_auto_prefiix_,        \
                              TestSuite::END_OF_TEST );

