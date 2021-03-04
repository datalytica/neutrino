#include <perspective/scalar.h>
#include <perspective/table.h>
#include <iostream>

using namespace perspective;

int
main()
{
    auto ss = mktscalar("hello");
    t_table tbl(t_schema{{"s", "i"}, {DTYPE_STR, DTYPE_INT64}});
    tbl.init();
    tbl.extend(2);
    tbl["s"]->set_scalar(0, mktscalar("hi"));
    tbl["s"]->set_scalar(1, mktscalar("hello"));
    tbl.pprint();
    auto col = tbl["s"];
    auto c2 = tbl["i"];
    auto d2 = c2->_get_data_lstore();
    int x;
    auto nll = mknull(DTYPE_FLOAT64);
    std::cout << nll << std::endl;
    return 0;
}