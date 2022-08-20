import pandas as pd1
from traceback import format_exc
import my_pd as pd2

tests = [
    lambda pd, _: pd.Series(list(range(5))),
    lambda pd, _: pd.Series(["Hello, World!", None], name="θεαρτ"),
    lambda pd, _: pd.Series("abc", name=""),
    lambda pd, _: pd.DataFrame({"Name": ["Braund, Mr. Owen Harris", "Allen, Mr. William Henry", "Bonnell, Miss. Elizabeth"], "Age": [22, 35, 58], "Sex": ["male", "male", "female"]}),
    lambda pd, data: data[-1]["Age"],
    lambda pd, data: data[-1].max(),
    lambda pd, data: pd.Series([], dtype=str).max(),
    lambda pd, data: pd.DataFrame({"What is love": [1, 2, -3], "oh": ["ohnteagahoenrt", "nothrea", "aeh"]}),
    lambda pd, data: pd.read_csv("courses.csv"),
    lambda pd, data: list(data[-1].dtypes),
    lambda pd, data: pd.DataFrame({"Cources": ["Spark", "Pandas", "Java", "Python", "PHP"], "Fee": ["25000", "20000", "15000", "15000", "18000"], "Duration": ["50 Days", "35 Days", "NaN", "30 Days", "30 Days"], "Discount": ["2000", "1000", "800", "500", "800"]}),
    lambda pd, data: pd.DataFrame({"very_long_name_A": ["Spark", "Pandas", "Java"], "very_long_name_B": ["25000", "20000", "15000"], "very_long_name_C": ["50 Days", "35 Days", "NaN"]}),
    lambda pd, data: pd.DataFrame({"long_E": [1, -2], "very_really_long_F": ["24", "34"]}),
    lambda pd, data: pd.DataFrame({"long_E": [1, 2], "very_really_long_F": [True, None]}),
    lambda pd, data: list(data[-1].dtypes),
    lambda pd, data: pd.read_csv("type_check.csv"),
    lambda pd, data: list(data[-1].dtypes),
    lambda pd, data: pd.Series([1, 2.3, -4.567, "789.112"]),
    lambda pd, data: pd.Series([1, 2.3, -4.567, 789.112]),
    lambda pd, data: pd.Series([1, 2.3, -4.567, 789.112 * 10**100]),
    lambda pd, data: data[-1].abs(),
    lambda pd, data: pd.Series([1, 2.3, 4.56]).to_string(),
    lambda pd, data: pd.Series([1, 2.3, -4.56]).to_string(),
    lambda pd, data: pd.Series([1, 23, -456]).to_string(),
    lambda pd, data: pd.Series(["A", "BC", True]).to_string(),
    lambda pd, data: pd.Series([False, True]).to_string(),
    lambda pd, data: pd.Series([1, "23", "-456"]).to_string(),
    lambda pd, data: pd.Series([1, 2.3, 4.56]).to_string(index=False),
    lambda pd, data: pd.Series([1, 2.3, -4.56]).to_string(index=False),
    lambda pd, data: pd.Series([1, 23, -456]).to_string(index=False),
    lambda pd, data: pd.Series(["A", "BC", True]).to_string(index=False),
    lambda pd, data: pd.Series([False, True]).to_string(index=False),
    lambda pd, data: pd.Series([1, "23", "-456"]).to_string(index=False),
    lambda pd, data: pd.Series([1, 23, str(10**49)]).to_string(),
    lambda pd, data: pd.Series([1, 23, True]).to_string(dtype=True, index=False),
    lambda pd, data: pd.Series([1, 23, True]).to_string(dtype=True, index=False),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(name=True),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(name=True, dtype=True),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(name=False, dtype=True),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(name=False, dtype=True),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(length=True),
    lambda pd, data: pd.Series([1, 23, True], name="A?").to_string(length=True, name=True, dtype=True),
    lambda pd, data: pd.Series(list(range(10))).to_string(max_rows=4, dtype=True),
    lambda pd, data: pd.Series(list(range(10))).to_string(max_rows=9, dtype=True, min_rows=3),
    lambda pd, data: pd.Series([1, 2.3, -4.56]).to_string(float_format=lambda f: f"{f:+.3f}"),
    lambda pd, data: pd.Series([1, 23, -456]).to_string(float_format=lambda f: f"{f:+.3f}"),
    lambda pd, data: pd.Series([1, 23, None]).to_string(),
    lambda pd, data: pd.Series({"aaa": 3, "bb": 2}, name="A?").to_string(header=True),
    lambda pd, data: pd.Series([22, -35, 58], name="Age"),
    lambda pd, data: pd.Series(list(range(61))),
    lambda pd, data: pd.Series([1, 2.3, -4.567, 789101112]),
    lambda pd, data: data[-1].mean(),
    lambda pd, data: data[-2].std(),
    lambda pd, data: data[-3].min(),
]


class COLORS:
    FAIL = "\033[91m"
    ENDC = "\033[0m"


history, errors = [[], []], 0
for test_index, test in enumerate(tests):
    print(" =" * 16, test_index + 1, "= " * 16)
    outs = set()
    for i, pd in enumerate((pd1, pd2)):
        try:
            y = test(pd, history[i])
        except Exception as e:
            y = format_exc()
        history[i].append(y)
        out = str(y).replace("\n", " |\n")
        outs.add(out)
        print(out, ("\n" + " -" * 32) * (i == 0))
    if len(outs) > 1:
        print(COLORS.FAIL + "A discrepancy was found!" + COLORS.ENDC)
        errors += 1
if errors:
    print(COLORS.FAIL + "Total errors: " + str(errors) + COLORS.ENDC)
