#include "testclass.h"
#include <limits>
namespace mytest{
    void TestClass::setIntToMax(uint64_t& x){
        x = std::numeric_limits<uint64_t>::max();
    }
};
