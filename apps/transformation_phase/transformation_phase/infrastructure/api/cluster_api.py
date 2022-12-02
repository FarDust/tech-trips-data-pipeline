from random import random
from typing import Dict, Tuple

from numpy import concatenate, ones, radians, unique
from pandas import DataFrame, concat, to_datetime
from sklearn.mixture import BayesianGaussianMixture
from sklearn.preprocessing import OneHotEncoder
from transformation_phase.constants.columns import Columns
from transformation_phase.constants.domain import CIRCLE_RADIUS
from transformation_phase.constants.environment import (
    MODEL_MAX_CLUSTERS,
    MODEL_MAX_ITERS,
    MODEL_RANDOM_STATE,
)
from transformation_phase.domain.entities.classification_data import (
    ClassificationData,
)
from transformation_phase.domain.ports.clustering_api_port import ClusteringAPI
from transformation_phase.utils.distance import haversine_vectorize


class ClusterClassificationAPI(ClusteringAPI):
    """With more time, this service is only a connector to another service"""

    __models: Dict[Tuple[int, str, int], BayesianGaussianMixture] = {}

    def train(self, entity: ClassificationData) -> None:
        data = entity.data.copy()

        data[Columns.DATETIME] = to_datetime(
            data[Columns.DATETIME]
        ).sort_values()

        for hour in unique(data[Columns.DATETIME].dt.hour):
            for region in unique(data[Columns.REGION]):
                data_batch = data.loc[
                    (data[Columns.DATETIME].dt.hour == hour)
                    & (data[Columns.REGION] == region),
                    :,
                ].copy()

                model_size = min(data_batch.shape[0], MODEL_MAX_CLUSTERS)
                if data_batch.empty:
                    continue

                (
                    _,
                    classification_df,
                ) = self.__get_classification_data(data_batch)

                if classification_df.shape[0] >= 2:
                    self.__fit_model(
                        classification_df, hour, region, model_size
                    )

    def classify(self, entity: ClassificationData) -> ClassificationData:

        data = entity.data.copy()

        data[Columns.DATETIME] = to_datetime(
            data[Columns.DATETIME]
        ).sort_values()

        results = []
        for hour in unique(data[Columns.DATETIME].dt.hour):
            for region in unique(data[Columns.REGION]):
                data_batch = data.loc[
                    (data[Columns.DATETIME].dt.hour == hour)
                    & (data[Columns.REGION] == region),
                    :,
                ].copy()
                if data_batch.empty:
                    continue

                (
                    clean_df,
                    classification_df,
                ) = self.__get_classification_data(data_batch)

                model_size = min(data_batch.shape[0], MODEL_MAX_CLUSTERS)

                if (hour, region, model_size) in self.__models.keys():
                    model = self.__models[(hour, region, model_size)]
                    classified_trips = self.__predict(classification_df, model)
                else:
                    classified_trips = classification_df.copy()
                    classified_trips[Columns.CLUSTER] = 0
                data_batch[Columns.DATETIME] = (
                    data_batch[Columns.DATETIME].view("int64") / 1e9
                )

                result_data = self.__merge_data(
                    data_batch, classified_trips, clean_df
                )

                if result_data.isnull().values.any():
                    print(result_data[result_data.isnull().any(axis=1)])
                    print(result_data.shape)
                    raise ValueError("Null values in result data")

                results.append(result_data)

        return ClassificationData(
            data=concat(results).reset_index().drop("index", axis=1).fillna(0),
        )

    def __get_classification_data(
        self, data: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        clean_df = data.loc[
            :,
            [Columns.ORIGIN_COORD, Columns.DESTINATION_COORD, Columns.DATETIME],
        ]

        # Separate point coordinates
        splitted_origin = (
            clean_df[Columns.ORIGIN_COORD]
            .str.strip(" ")
            .str.split(pat=" ", expand=True)
        )
        splitted_destination = (
            clean_df[Columns.DESTINATION_COORD]
            .str.strip(" ")
            .str.split(pat=" ", expand=True)
        )
        clean_df[Columns.ORIGIN_LON] = (
            splitted_origin[1].str.replace("(", "", regex=False).astype(float)
        )
        clean_df[Columns.ORIGIN_LAT] = (
            splitted_origin[2].str.replace(")", "", regex=False).astype(float)
        )
        clean_df[Columns.DESTINATION_LON] = (
            splitted_destination[1]
            .str.replace("(", "", regex=False)
            .astype(float)
        )
        clean_df[Columns.DESTINATION_LAT] = (
            splitted_destination[2]
            .str.replace(")", "", regex=False)
            .astype(float)
        )

        # Get hour of the day
        clean_df[Columns.HOUR] = clean_df[Columns.DATETIME].dt.hour

        clean_df.drop(
            [Columns.ORIGIN_COORD, Columns.DESTINATION_COORD, Columns.DATETIME],
            axis=1,
            inplace=True,
        )

        classification_df = clean_df.copy()
        classification_df.pop(Columns.HOUR)
        classification_df[Columns.DESTINATION_LAT] = radians(
            clean_df[Columns.DESTINATION_LAT]
        )
        classification_df[Columns.DESTINATION_LON] = radians(
            clean_df[Columns.DESTINATION_LON]
        )
        classification_df[Columns.ORIGIN_LAT] = radians(
            clean_df[Columns.ORIGIN_LAT]
        )
        classification_df[Columns.ORIGIN_LON] = radians(
            clean_df[Columns.ORIGIN_LON]
        )

        classification_df[Columns.DISTANCE] = haversine_vectorize(
            classification_df[Columns.ORIGIN_LON],
            classification_df[Columns.ORIGIN_LAT],
            classification_df[Columns.DESTINATION_LON],
            classification_df[Columns.DESTINATION_LAT],
        )

        return clean_df, classification_df

    def __fit_model(
        self, data: DataFrame, hour: int, region: str, model_size: int
    ) -> None:
        if (hour, region, model_size) in self.__models.keys():
            model = self.__models[(hour, region, model_size)]
        else:
            model = BayesianGaussianMixture(
                init_params="k-means++",
                n_components=min(data.shape[0], MODEL_MAX_CLUSTERS),
                max_iter=MODEL_MAX_ITERS,
                random_state=MODEL_RANDOM_STATE,
                warm_start=True,
            )
        model.fit(data)
        self.__models[
            (hour, region, min(data.shape[0], MODEL_MAX_CLUSTERS))
        ] = model

    def __predict(
        self, data: DataFrame, model: BayesianGaussianMixture
    ) -> DataFrame:

        classified_trips = data.copy()
        classified_trips = DataFrame(
            classified_trips.values,
            columns=classified_trips.columns,
            index=classified_trips.index,
        )

        classified_trips[Columns.CLUSTER] = model.predict(data)
        return classified_trips

    def __merge_data(
        self,
        original_data: DataFrame,
        classified_trips: DataFrame,
        clean_df: DataFrame,
    ):
        datasource = original_data.loc[:, [Columns.DATASOURCE]]
        base_data = (
            classified_trips.loc[:, [Columns.CLUSTER]]
            .join(original_data)
            .join(clean_df)
            .groupby([Columns.CLUSTER, Columns.REGION, Columns.HOUR])[
                [
                    Columns.ORIGIN_LAT,
                    Columns.ORIGIN_LON,
                    Columns.DESTINATION_LAT,
                    Columns.DESTINATION_LON,
                    Columns.DATETIME,
                ]
            ]
            .mean()
            .dropna()
        )

        enc = OneHotEncoder()
        enc.fit(original_data[Columns.DATASOURCE].values.reshape(-1, 1))
        encoded_categories = enc.transform(
            original_data[Columns.DATASOURCE].values.reshape(-1, 1)
        ).toarray()

        source = (
            classified_trips.loc[:, [Columns.CLUSTER]]
            .join(original_data)
            .join(clean_df)
            .loc[:, [Columns.CLUSTER, Columns.REGION, Columns.HOUR]]
            .join(DataFrame(encoded_categories, index=original_data.index))
            .groupby([Columns.CLUSTER, Columns.REGION, Columns.HOUR])
            .sum()
            .dropna()
        )

        for datasource in range(enc.categories_[0].shape[0]):
            source.rename(
                columns={datasource: enc.categories_[0][datasource]},
                inplace=True,
            )
            source[enc.categories_[0][datasource]] = source[
                enc.categories_[0][datasource]
            ].astype(int)

        final_data = base_data.join(source)
        final_data[Columns.DATETIME] = to_datetime(
            final_data[Columns.DATETIME], unit="s"
        )
        final_data = final_data.reset_index()
        final_data.drop(Columns.CLUSTER, axis=1, inplace=True)

        return final_data
