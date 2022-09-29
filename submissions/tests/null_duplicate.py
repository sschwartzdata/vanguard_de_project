
def null_count(df_list):
    """
    Checks each dataframe for nulls.
    Parameters:
    df_list:  list of (dataframes, name) pairs to check for nulls
    """
    for df, name in df_list:
        print(name + "table NULLS : ")
        print(df.isna().sum())


def duplicate_count(df_list):
    """
    Checks each dataframe for nulls.
    Parameters:
    df_list:  list of (dataframes, name) pairs to check for duplicates
    """
    for df, name in df_list:
        print(name + "table duplicates : ")
        print(df.duplicated().sum())
