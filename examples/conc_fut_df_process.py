import os
import time
import pandas as pd
import numpy as np
from pathlib import Path
import functools
import concurrent.futures


def path_up_dir(level, char):
    """Returns a string of path up n levels, replaces backslash sperator with char 
    
    Parameters
    ----------
    level
        The number of levels up to go (int)
    char
        Chracter to replace sperator with (str)
    
    Returns
    -------
        The path one level up, with seperators replaced by char (str)
    
    """
    return str(Path(os.getcwd()).parents[level]).replace(os.sep, char)


def df_col_str_replacer(df, col, input_dict):
    """Replaces all intstances of str in input dict with values for selected column in df
    
    Parameters
    ----------
    df
        The input df (df)
    col
        The selected column (str)
    input_dic
        Input dictionary indicating string values and what they should be replaced by (dict)
    Returns
    -------
        The original df but values in col have been replaced (df)
    """
    return df.replace({col: input_dict})


def main_branch_simplefy(df):
    """Replaces values in column main branch
    
    Parameters
    ----------
    df
        The input survey data df (df)
    Returns
    -------
        Input df with values in column MainBranch replaced (df)
    """
    return df_col_str_replacer(
        df,
        "MainBranch",
        {
            "I am a developer by profession": "developer",
            "I am a student who is learning to code": "student",
            "I am not primarily a developer, but I write code sometimes as part of my work": "user",
            "I code primarily as a hobby": "hobbyist",
            "I used to be a developer by profession,"
            " but no longer am": "ex-developer",
            "None of these": "Other",
        },
    )


def rand_val_col_adder(df, col_name, lower_val, upper_val):
    """Adds a column with name col_name, containing random ints to df
    
    Parameters
    ----------
    df
        The input df (df)
    col_name
        The name of the column to be added (str)
    lower_val
        The lower bound of the random numbers generated (int)
    upper_val
        The upper bound of the random numbers generated (int)

    Returns
    -------
        Original df but with column (name col_name) added containg random integers betweeb lower_val
        and upper_val (df)
    """
    rand_array = np.random.randint(lower_val, upper_val, size=(len(df), 1))
    df_rand_col = pd.DataFrame(rand_array, columns=[col_name])
    return df.join(df_rand_col)


def arbitrary_maths_operation(num, pow):
    """Returns input number to the power of pow
    
    Parameters
    ----------
    num
        The number to be multiplied (int)
    pow
        The number indicating to the power of (int)

    Returns
    -------
        An iteger of num to the power of pow (int)
    
    """
    return num ** pow


if __name__ == "__main__":

    ROOT_DIR = path_up_dir(0, "/")
    DATA_DIR = f"{ROOT_DIR}/parallel/data/"
    OUT_DATA_DIR = f"{ROOT_DIR}/parallel/data_processed/"
    SURVEYS_FILE = "survey_results.csv"

    MATH_COL = "Points"
    REPEAT_NO = 5
    LOWER_VAL = 0
    UPPER_VAL = 100
    POWER = 99

    survey_data = pd.read_csv(
        os.path.join(DATA_DIR, SURVEYS_FILE), sep=",", low_memory=False
    )
    survey_data_with_pts = rand_val_col_adder(
        survey_data, MATH_COL, LOWER_VAL, UPPER_VAL
    )
    # Adding points column which is rand int from 0 to 100

    arbitraty_num_calc = functools.partial(
        arbitrary_maths_operation, pow=POWER
    )  # setting power so it remains constant as function is mapped

    start = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        survey_data_with_pts["Points"] = survey_data_with_pts["Points"].map(
            arbitraty_num_calc
        )
        survey_data_with_pts["Netherlands"] = survey_data_with_pts.Country.apply
        (lambda x: "yes" if x in ["Netherlands"] else "no")
        survey_data_with_pts = main_branch_simplefy(survey_data_with_pts)

    end = time.perf_counter()
    print(f"parallel operation took {end - start} seconds")

    survey_data_with_pts.to_csv(
        os.path.join(OUT_DATA_DIR, f"{SURVEYS_FILE[0:-4]}_processed.csv")
    )
    print(f"export of {SURVEYS_FILE[0:-4]}_processed.csv complete")
