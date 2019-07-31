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

#include "nuraft.hxx"
#include "strfmt.hxx"

#include "test_common.h"

using namespace nuraft;

namespace strfmt_test {

int strfmt_basic_test() {

    CHK_EQ( std::string( "number 10" ),
            std::string( strfmt<20>("number %d").fmt(10) ) );
    CHK_EQ( std::string( "another string" ),
            std::string( strfmt<30>("another %s").fmt("string") ) );
    CHK_EQ( std::string( "100-20=80" ),
            std::string( strfmt<30>("%d-%d=%d").fmt(100, 20, 80) ) );

    return 0;
}

}  // namespace strfmt_test;
using namespace strfmt_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

    ts.doTest( "strfmt basic test",
               strfmt_basic_test );

    return 0;
}


