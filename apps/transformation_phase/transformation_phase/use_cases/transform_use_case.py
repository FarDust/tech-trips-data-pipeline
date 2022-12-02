from hashlib import md5
from json import dumps
from pathlib import Path

from pandas.util import hash_pandas_object
from transformation_phase.constants.environment import PROCESSED_PATH
from transformation_phase.domain.entities.log_data import LogData
from transformation_phase.domain.entities.output_data_file import OutputDataFile
from transformation_phase.domain.ports.log_storage import LogStorage
from transformation_phase.domain.ports.notifications_port import NotificationsPort
from transformation_phase.domain.ports.output_storage_port import OutputStorage


class TransformUseCase:
    def __init__(
        self,
        output_storage: OutputStorage,
        log_storage: LogStorage,
        notifications_api: NotificationsPort,
    ) -> None:
        self.output_storage = output_storage
        self.log_storage = log_storage
        self.notifications_api = notifications_api

    def transform(self, entity: OutputDataFile) -> None:
        base_path = Path(entity.name.replace(f"{PROCESSED_PATH}/", ""))
        new_path_name = base_path.with_stem(f"{Path(base_path.name).stem}")

        batch_hash = md5(hash_pandas_object(entity.data).values).hexdigest()
        try:
            self.log_storage.create(
                LogData(batch_hash=batch_hash, source=str(new_path_name))
            )

            self.notifications_api.notify(
                dumps(
                    {
                        "code": "LOCKED",
                        "message": f"Batch {batch_hash} locked",
                        "file": str(new_path_name),
                    }
                )
            )
        except ValueError:
            self.notifications_api.notify(
                dumps(
                    {
                        "code": "DUPLICATED",
                        "message": f"Batch {batch_hash} duplicated, skipping",
                        "file": str(new_path_name),
                    }
                )
            )
        except Exception as e:
            self.log_storage.delete(
                LogData(batch_hash=batch_hash, source=str(new_path_name))
            )
            self.notifications_api.notify(
                dumps(
                    {
                        "code": "UNLOCKED",
                        "message": str(e),
                        "file": str(new_path_name),
                    }
                )
            )
            raise e
        else:
            new_data = self.output_storage.create(
                OutputDataFile(name=str(new_path_name), data=entity.data)
            )

            self.notifications_api.notify(
                dumps(
                    {
                        "code": "SUCCESS",
                        "message": f"Batch {batch_hash} successfully processed",
                        "file": new_data.name,
                    }
                )
            )
        finally:
            self.notifications_api.notify(
                dumps(
                    {
                        "code": "DONE",
                        "message": f"Batch {batch_hash} done",
                        "file": str(new_path_name),
                    }
                )
            )
