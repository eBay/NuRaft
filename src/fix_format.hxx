#pragma once

#include <inttypes.h>
#include <string>

#if __WORDSIZE == 32
static void replace_all(std::string& s, const std::string& search, const std::string& target) {
    size_t index = 0;
    while (true) {
        index = s.find(search, index);
        if (index == std::string::npos) break;
        s.replace(index, search.size(), target);
        index += search.size();
    }
}

#define _fix_format(format)                                                                                            \
    std::string format_string(format);                                                                                 \
    replace_all(format_string, "%ld", "%" PRId64);                                                                     \
    replace_all(format_string, "%lu", "%" PRIu64);                                                                     \
    format = format_string.c_str();
#else
#define _fix_format(format)
#endif
