from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Identity,
    Integer,
    MetaData,
    Table,
    Text,
    inspect,
)
from sqlalchemy.engine import Engine
from transformation_phase.constants.columns import Columns
from transformation_phase.domain.entities.output_data_file import OutputDataFile
from transformation_phase.domain.ports.output_storage_port import OutputStorage
from transformation_phase.infrastructure.storage.shared.base_storage import (
    AbstractStorage,
)


class SQLTripsOutputStorage(AbstractStorage[OutputDataFile], OutputStorage):
    """Storage class for output files."""

    def __init__(self, engine: Engine, table="trips") -> None:
        self.engine = engine
        self.table = table
        if not inspect(self.engine).has_table(self.table):
            metadata = MetaData(engine)
            Table(
                self.table,
                metadata,
                Column("pk", Integer, Identity(), primary_key=True),
                Column(Columns.REGION, Text, index=True),
                Column(Columns.HOUR, Integer),
                Column(Columns.ORIGIN_LAT, Float),
                Column(Columns.ORIGIN_LON, Float),
                Column(Columns.DESTINATION_LAT, Float),
                Column(Columns.DESTINATION_LON, Float),
                Column(Columns.DATETIME, DateTime),
            )

            metadata.create_all()

    def create(self, entity: OutputDataFile) -> OutputDataFile:

        entity.data.loc[
            :,
            [
                Columns.REGION,
                Columns.HOUR,
                Columns.ORIGIN_LAT,
                Columns.ORIGIN_LON,
                Columns.DESTINATION_LAT,
                Columns.DESTINATION_LON,
                Columns.DATETIME,
            ],
        ].to_sql(
            self.table,
            if_exists="append",
            index=False,
            chunksize=10000,
            con=self.engine,
            method="multi",
        )

        return OutputDataFile(name=entity.name, data=entity.data)

    def find(self, entity: OutputDataFile) -> OutputDataFile:
        pass
