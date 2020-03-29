from pandas import DataFrame


def create_features(save_filename: str, df: DataFrame = None) -> DataFrame:
    import pandas as pd

    cols_to_keep = ['living_area', 'additional_area', 'plot_area', 'rooms', 'rent', 'floor',
                    'construction_year', 'object_type', 'published', 'sold_date', 'sold_price', 'district']

    df = df[cols_to_keep]

    columns_to_one_hot = ['object_type', 'district']
    dummies_df = pd.get_dummies(df, columns=columns_to_one_hot, prefix=['otype', 'district'])  # add areas later

    df.drop(columns=columns_to_one_hot, inplace=True)
    df = pd.concat([df, dummies_df], axis=1)

    # TODO: Look into how to OHE areas in GBG
    # TODO: Look into how to OHE seller

    df.to_pickle(f'data/processed/{save_filename}.pkl')

    return df
