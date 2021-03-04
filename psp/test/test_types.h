struct t_tr_i64_tr_i64
{
    typedef tr_i64 pkey;
    typedef tr_i64 value;
};

struct t_tr_i64_tr_i32
{
    typedef tr_i64 pkey;
    typedef tr_i32 value;
};

struct t_tr_i64_tr_u64
{
    typedef tr_i64 pkey;
    typedef tr_u64 value;
};

struct t_tr_i64_tr_float64
{
    typedef tr_i64 pkey;
    typedef tr_float64 value;
};

struct t_tr_i64_tr_date
{
    typedef tr_i64 pkey;
    typedef tr_date value;
};

struct t_tr_i64_tr_time
{
    typedef tr_i64 pkey;
    typedef tr_time value;
};

struct t_tr_i64_tr_str
{
    typedef tr_i64 pkey;
    typedef tr_str value;
};

struct t_tr_i32_tr_i64
{
    typedef tr_i32 pkey;
    typedef tr_i64 value;
};

struct t_tr_i32_tr_i32
{
    typedef tr_i32 pkey;
    typedef tr_i32 value;
};

struct t_tr_i32_tr_u64
{
    typedef tr_i32 pkey;
    typedef tr_u64 value;
};

struct t_tr_i32_tr_float64
{
    typedef tr_i32 pkey;
    typedef tr_float64 value;
};

struct t_tr_i32_tr_date
{
    typedef tr_i32 pkey;
    typedef tr_date value;
};

struct t_tr_i32_tr_time
{
    typedef tr_i32 pkey;
    typedef tr_time value;
};

struct t_tr_i32_tr_str
{
    typedef tr_i32 pkey;
    typedef tr_str value;
};

struct t_tr_u64_tr_i64
{
    typedef tr_u64 pkey;
    typedef tr_i64 value;
};

struct t_tr_u64_tr_i32
{
    typedef tr_u64 pkey;
    typedef tr_i32 value;
};

struct t_tr_u64_tr_u64
{
    typedef tr_u64 pkey;
    typedef tr_u64 value;
};

struct t_tr_u64_tr_float64
{
    typedef tr_u64 pkey;
    typedef tr_float64 value;
};

struct t_tr_u64_tr_date
{
    typedef tr_u64 pkey;
    typedef tr_date value;
};

struct t_tr_u64_tr_time
{
    typedef tr_u64 pkey;
    typedef tr_time value;
};

struct t_tr_u64_tr_str
{
    typedef tr_u64 pkey;
    typedef tr_str value;
};

struct t_tr_float64_tr_i64
{
    typedef tr_float64 pkey;
    typedef tr_i64 value;
};

struct t_tr_float64_tr_i32
{
    typedef tr_float64 pkey;
    typedef tr_i32 value;
};

struct t_tr_float64_tr_u64
{
    typedef tr_float64 pkey;
    typedef tr_u64 value;
};

struct t_tr_float64_tr_float64
{
    typedef tr_float64 pkey;
    typedef tr_float64 value;
};

struct t_tr_float64_tr_date
{
    typedef tr_float64 pkey;
    typedef tr_date value;
};

struct t_tr_float64_tr_time
{
    typedef tr_float64 pkey;
    typedef tr_time value;
};

struct t_tr_float64_tr_str
{
    typedef tr_float64 pkey;
    typedef tr_str value;
};

struct t_tr_date_tr_i64
{
    typedef tr_date pkey;
    typedef tr_i64 value;
};

struct t_tr_date_tr_i32
{
    typedef tr_date pkey;
    typedef tr_i32 value;
};

struct t_tr_date_tr_u64
{
    typedef tr_date pkey;
    typedef tr_u64 value;
};

struct t_tr_date_tr_float64
{
    typedef tr_date pkey;
    typedef tr_float64 value;
};

struct t_tr_date_tr_date
{
    typedef tr_date pkey;
    typedef tr_date value;
};

struct t_tr_date_tr_time
{
    typedef tr_date pkey;
    typedef tr_time value;
};

struct t_tr_date_tr_str
{
    typedef tr_date pkey;
    typedef tr_str value;
};

struct t_tr_time_tr_i64
{
    typedef tr_time pkey;
    typedef tr_i64 value;
};

struct t_tr_time_tr_i32
{
    typedef tr_time pkey;
    typedef tr_i32 value;
};

struct t_tr_time_tr_u64
{
    typedef tr_time pkey;
    typedef tr_u64 value;
};

struct t_tr_time_tr_float64
{
    typedef tr_time pkey;
    typedef tr_float64 value;
};

struct t_tr_time_tr_date
{
    typedef tr_time pkey;
    typedef tr_date value;
};

struct t_tr_time_tr_time
{
    typedef tr_time pkey;
    typedef tr_time value;
};

struct t_tr_time_tr_str
{
    typedef tr_time pkey;
    typedef tr_str value;
};

struct t_tr_str_tr_i64
{
    typedef tr_str pkey;
    typedef tr_i64 value;
};

struct t_tr_str_tr_i32
{
    typedef tr_str pkey;
    typedef tr_i32 value;
};

struct t_tr_str_tr_u64
{
    typedef tr_str pkey;
    typedef tr_u64 value;
};

struct t_tr_str_tr_float64
{
    typedef tr_str pkey;
    typedef tr_float64 value;
};

struct t_tr_str_tr_date
{
    typedef tr_str pkey;
    typedef tr_date value;
};

struct t_tr_str_tr_time
{
    typedef tr_str pkey;
    typedef tr_time value;
};

struct t_tr_str_tr_str
{
    typedef tr_str pkey;
    typedef tr_str value;
};

typedef Types<t_tr_i64_tr_i64, t_tr_i64_tr_i32, t_tr_i64_tr_u64,
    t_tr_i64_tr_float64, t_tr_i64_tr_date, t_tr_i64_tr_time, t_tr_i64_tr_str,
    t_tr_i32_tr_i64, t_tr_i32_tr_i32, t_tr_i32_tr_u64, t_tr_i32_tr_float64,
    t_tr_i32_tr_date, t_tr_i32_tr_time, t_tr_i32_tr_str, t_tr_u64_tr_i64,
    t_tr_u64_tr_i32, t_tr_u64_tr_u64, t_tr_u64_tr_float64, t_tr_u64_tr_date,
    t_tr_u64_tr_time, t_tr_u64_tr_str, t_tr_float64_tr_i64, t_tr_float64_tr_i32,
    t_tr_float64_tr_u64, t_tr_float64_tr_float64, t_tr_float64_tr_date,
    t_tr_float64_tr_time, t_tr_float64_tr_str, t_tr_date_tr_i64,
    t_tr_date_tr_i32, t_tr_date_tr_u64, t_tr_date_tr_float64, t_tr_date_tr_date,
    t_tr_date_tr_time, t_tr_date_tr_str, t_tr_time_tr_i64, t_tr_time_tr_i32,
    t_tr_time_tr_u64, t_tr_time_tr_float64, t_tr_time_tr_date,
    t_tr_time_tr_time, t_tr_time_tr_str, t_tr_str_tr_i64, t_tr_str_tr_i32,
    t_tr_str_tr_u64, t_tr_str_tr_float64, t_tr_str_tr_date, t_tr_str_tr_time,
    t_tr_str_tr_str>
    all_test_traits;
