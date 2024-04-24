/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

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

#include "libnuraft/logger.hxx"
#include "tracer.hxx"

#include "test_common.h"
#include <cstddef>
#include <iostream>
#include <sstream>
#include <string>

using namespace nuraft;

namespace logger_test {

class ToStringStreamLogger : public nuraft::logger {
public:
    ToStringStreamLogger() = default;

    ~ToStringStreamLogger() = default;

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& msg) override {

        static const char* const lv_names[7] = {
            "====", "FATL", "ERRO", "WARN", "INFO", "DEBG", "TRAC"};

        ss << lv_names[level] << " [" << source_file << ":" << line_number << ", "
           << func_name << "]" << msg << "\n";
    }

    std::string to_str() const { return ss.str(); }

    std::stringstream ss;
};

int logger_basic_test() {

    ToStringStreamLogger logger;
    nuraft::logger* l_ = &logger;
    p_db("hello %s", "world");

    std::cout << logger.to_str();
    return 0;
}

int logger_long_line_test() {

    for (int i = 1800; i < 4096; ++i) {
        const std::string str(i, 'a');

        ToStringStreamLogger logger;
        nuraft::logger* l_ = &logger;
        p_db("long string: %s", str.c_str());

        const std::string log_str = logger.to_str();
        CHK_GT(log_str.size(), str.size());
        // std::cout << log_str;
    }

    for (int i = 1; i < 1024; i *= 2) {
        const std::string str(1024UL * i, 'a');

        ToStringStreamLogger logger;
        nuraft::logger* l_ = &logger;
        p_db("long string: %s", str.c_str());

        const std::string log_str = logger.to_str();
        CHK_GT(log_str.size(), str.size());
        // std::cout << log_str;
    }

    return 0;
}

} // namespace logger_test
using namespace logger_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest("logger basic test", logger_basic_test);

    ts.doTest("logger long line test", logger_long_line_test);

    return 0;
}

