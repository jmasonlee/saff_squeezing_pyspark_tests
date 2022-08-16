import functools

#
def add_empty_dfs(func, *args, **kwargs):
    def inner(applesauce):
        @functools.wraps(func)
        def wrapper_do_twice(*args, **kwargs):
            func(*args, **kwargs)
            return func(*args, **kwargs)
        return wrapper_do_twice
    return inner

def test_decorator(spark):
    @add_empty_dfs(applesauce='important_df')
    def function_under_test(important_df, empty_df):
        assert important_df.columns == empty_df.columns
        assert empty_df.count() == 0

    important_df = spark.createDataFrame(data=[{'key': 'value'}])
    empty_df = spark.createDataFrame(data=[{}])
    function_under_test(important_df=important_df, empty_df=empty_df)

#Balazs idea

#with empty_dataframe_form(important_df) as empy:
#    transform(...)