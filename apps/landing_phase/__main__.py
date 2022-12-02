from landing_phase.constants.storage import ProtocolType
from landing_phase.infrastructure.api.local_notifications import LocalNotifications
from landing_phase.infrastructure.storage.input_storage import InputFileStorage
from landing_phase.infrastructure.storage.output_storage import OutputFileStorage
from landing_phase.use_cases.transform_use_case import TransformUseCase

if __name__ == "__main__":
    from pathlib import Path

    bucket_path = str(Path(__file__).parents[2].resolve().absolute() / "data")

    use_cases = {
        "transform": TransformUseCase(
            input_storage=InputFileStorage(protocol=ProtocolType.LOCAL),
            output_storage=OutputFileStorage(
                bucket_name=bucket_path, protocol=ProtocolType.LOCAL
            ),
            notifications_api=LocalNotifications(),
        )
    }

    use_case = use_cases.get("transform")

    if use_case:
        use_case.transform(
            bucket_path,
            "trips.csv",
        )
