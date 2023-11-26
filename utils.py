import pandas as pd


def feature_engineering(raw_data: pd.DataFrame) -> pd.DataFrame:
    preprocessed_data = raw_data.copy(deep=True)
    preprocessed_data.columns = ["age", "job", "marital", "education", "default", "balance", "housing", "loan", "contract", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "class"]

    # data preprocessing
    preprocessed_data["has_default"] = preprocessed_data.default.apply(
        lambda x: 0 if x == "no" else 1 if x == "yes" else -1
    )

    preprocessed_data["has_housing_loan"] = preprocessed_data.housing.apply(
        lambda x: 0 if x == "no" else 1 if x == "yes" else -1
    )

    preprocessed_data["has_personal_loan"] = preprocessed_data.loan.apply(
        lambda x: 0 if x == "no" else 1 if x == "yes" else -1
    )
    category_columns = ["job", "marital", "education",  "contract",  "month"]

    for col in category_columns:

        col_dummies = pd.get_dummies(preprocessed_data[col], prefix=col)
        preprocessed_data = pd.concat([preprocessed_data, col_dummies], axis=1)

    # other preprocessing attributes
    poutcome_dummies =pd.get_dummies(preprocessed_data.poutcome, prefix="prev_camp_outcome")
    preprocessed_data = pd.concat([preprocessed_data, poutcome_dummies], axis=1)

    preprocessed_data.drop(columns=["poutcome", "default", "housing", "loan"] + category_columns, inplace=True)

    # target preprocessing
    preprocessed_data["target"] = preprocessed_data["class"].apply(lambda x: 0 if x == "1" else 1)
    preprocessed_data.drop(columns=["class"], inplace=True)

    return preprocessed_data