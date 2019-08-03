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

#ifndef _LOGGER_HXX_
#define _LOGGER_HXX_

namespace nuraft {

class logger {
    __interface_body__(logger);

public:
    /**
     * (Deprecated)
     * Put a debug level log.
     *
     * @param log_line Contents of the log.
     */
    virtual void debug(const std::string& log_line) {}

    /**
     * (Deprecated)
     * Put an info level log.
     *
     * @param log_line Contents of the log.
     */
    virtual void info(const std::string& log_line) {}

    /**
     * (Deprecated)
     * Put a warning level log.
     *
     * @param log_line Contents of the log.
     */
    virtual void warn(const std::string& log_line) {}

    /**
     * (Deprecated)
     * Put an error level log.
     *
     * @param log_line Contents of the log.
     */
    virtual void err(const std::string& log_line) {}

    /**
     * Set the log level.
     * Only logs whose level is equal to or smaller than the log level
     * will be recorded.
     *
     * @param l New log level.
     */
    virtual void set_level(int l) {}

    /**
     * Get the current log level.
     *
     * @return Current log level.
     */
    virtual int  get_level() { return 6; }

    /**
     * Put a log with level, line number, function name,
     * and file name.
     *
     * Log level info:
     *    Trace:    6
     *    Debug:    5
     *    Info:     4
     *    Warning:  3
     *    Error:    2
     *    Fatal:    1
     *
     * @param level Level of given log.
     * @param source_file Name of file where the log is located.
     * @param func_name Name of function where the log is located.
     * @param line_number Line number of the log.
     * @param log_line Contents of the log.
     */
    virtual void put_details(int level,
                             const char* source_file,
                             const char* func_name,
                             size_t line_number,
                             const std::string& log_line) {}
};

}

#endif //_LOGGER_HXX_
