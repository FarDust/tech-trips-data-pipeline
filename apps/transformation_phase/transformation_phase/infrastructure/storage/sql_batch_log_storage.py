from pandas import DataFrame, read_sql
from sqlalchemy import Column, MetaData, String, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from transformation_phase.domain.entities.log_data import LogData
from transformation_phase.domain.ports.log_storage import LogStorage
from transformation_phase.infrastructure.storage.shared.base_storage import (
    AbstractStorage,
)


class SQLBatchLogStorage(AbstractStorage[LogData], LogStorage):
    """Storage class for output files."""

    def __init__(self, engine: Engine, table="batch_log") -> None:
        self.engine = engine
        self.table = table
        self.columns = ["batch_hash", "source"]
        if not inspect(self.engine).has_table(self.table):
            metadata = MetaData(engine)
            Table(
                self.table,
                metadata,
                Column(self.columns[0], String(32), primary_key=True),
                Column(self.columns[1], String(64), primary_key=True),
            )
            metadata.create_all()

    def create(self, entity: LogData) -> LogData:
        log_record = DataFrame(columns=self.columns, data=[entity])
        try:
            log_record.to_sql(
                self.table,
                if_exists="append",
                index=False,
                chunksize=10000,
                con=self.engine,
            )
        except IntegrityError:
            raise ValueError(f"Batch hash {entity.batch_hash} already exists.")

        return LogData(batch_hash=entity.batch_hash, source=entity.source)

    def find(self, entity: LogData) -> LogData:
        result_df = read_sql(
            f"SELECT * FROM {self.table} WHERE batch_hash = '{entity.batch_hash}'",
            con=self.engine,
        )

        if result_df.empty:
            raise ValueError(f"Batch hash {entity.batch_hash} not found.")

        return LogData(
            batch_hash=result_df[self.columns[0]].values[0],
            source=result_df[self.columns[1]].values[0],
        )

    def delete(self, entity: LogData) -> bool:
        result = self.engine.execute(
            f"DELETE FROM {self.table} WHERE batch_hash = '{entity.batch_hash}'"
        )
        return result.rowcount > 0
