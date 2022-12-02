from base64 import b64decode
from json import loads
from typing import TYPE_CHECKING

import functions_framework
from landing_phase.constants.storage import ProtocolType
from landing_phase.infrastructure.storage.input_storage import InputFileStorage
from landing_phase.infrastructure.storage.output_storage import OutputFileStorage
from landing_phase.use_cases.transform_use_case import TransformUseCase

if TYPE_CHECKING:
    from typing import TypedDict

    from google.cloud.functions.context import Context

    class GCSEventPayload(TypedDict):
        name: str
        bucket: str
        metageneration: str
        timeCreated: str
        updated: str

    class CloudEvent(TypedDict):
        data: str


use_cases = {
    "transform": TransformUseCase(
        input_storage=InputFileStorage(protocol=ProtocolType.GCS),
        output_storage=OutputFileStorage(
            bucket_name="landing-phase", protocol=ProtocolType.GCS
        ),
    )
}


@functions_framework.cloud_event
def lading_phase(cloud_event: "CloudEvent", context: "Context") -> None:
    """
    Background Cloud Function to be triggered by Cloud Storage.
    This generic function transforms data from a CSV file in a parquet file.
    """
    if "data" in cloud_event:
        payload: "GCSEventPayload" = loads(
            b64decode(cloud_event["data"]).decode("utf-8")
        )

        if "name" in payload and "bucket" in payload:
            bucket = payload["bucket"]
            name = payload["name"]

            use_case = use_cases.get("transform")

            if use_case:
                use_case.transform(
                    bucket,
                    name,
                )

    else:
        raise ValueError("No data in CloudEvent")
