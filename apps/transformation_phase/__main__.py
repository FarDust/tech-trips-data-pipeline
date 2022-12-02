from sqlalchemy.engine import create_engine
from transformation_phase.constants.environment import DB_CONNECTION_URL
from transformation_phase.constants.storage import ProtocolType
from transformation_phase.infrastructure.api.cluster_api import ClusterClassificationAPI
from transformation_phase.infrastructure.api.local_notifications import (
    LocalNotifications,
)
from transformation_phase.infrastructure.storage.input_storage import InputFileStorage
from transformation_phase.infrastructure.storage.sql_batch_log_storage import (
    SQLBatchLogStorage,
)
from transformation_phase.infrastructure.storage.sql_output_storage import (
    SQLTripsOutputStorage,
)
from transformation_phase.use_cases.clasify_use_case import ClassifyUseCase
from transformation_phase.use_cases.transform_use_case import TransformUseCase

if __name__ == "__main__":
    from pathlib import Path

    bucket_path = Path(__file__).parents[2].resolve().absolute() / "data"
    example_file = (
        Path("source=trips_transformed")
        # / "hash=8ee3e469366db947933d5d13ba355d26"
        # / "region=Hamburg"
        # / "33ba9dc7dd3e47a19163be37b09c6df3-0.parquet"
    )

    use_cases = {
        "transform": TransformUseCase(
            output_storage=SQLTripsOutputStorage(
                engine=create_engine(
                    DB_CONNECTION_URL,
                ),
            ),
            log_storage=SQLBatchLogStorage(
                engine=create_engine(
                    DB_CONNECTION_URL,
                ),
            ),
            notifications_api=LocalNotifications(),
        ),
        "classify": ClassifyUseCase(
            input_storage=InputFileStorage(protocol=ProtocolType.LOCAL),
            clustering_api=ClusterClassificationAPI(),
            notifications_api=LocalNotifications(),
        ),
    }

    classify_use_case = use_cases["classify"]
    transform_use_case = use_cases["transform"]

    result = classify_use_case.classify(bucket=str(bucket_path), name=str(example_file))

    for output_file in result:
        transform_use_case.transform(output_file)
