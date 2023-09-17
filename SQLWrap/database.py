import logging
import sqlite3
import traceback
import asqlite
import typing as t

from . import query, constants
from .datapath import get_datafile_path

class Column:
    def __init__(self, name: str, column_type: str, specialities: t.List[str]=None) -> None:
        if (specialities == None):
            specialities = []
            
        self.name = name
        self.type = column_type
        self.specialities = specialities

class Table:
    def __init__(self, table_name, primary_key_columns: t.Union[t.List, t.Any], *, columns: t.List[Column], database: str=None, auto_increment=False):
        """Unique key columns must be type of integer."""
        self.table = table_name

        if not isinstance(primary_key_columns, list):
            primary_key_columns = [primary_key_columns]
        self.primary_keys = primary_key_columns

        if database:
            if (constants.database_name == None):
                constants.database_name = database
            self.database_path = get_datafile_path(database)
        else:
            if (constants.databaseName == None):
                assert "Database name is not specified. Specify it with SQLWrap.databaseName or at constructor."
            self.database_path = get_datafile_path(constants.databaseName)

        self._create_table(auto_increment=auto_increment, columns=columns)
        
    def _create_table(self, auto_increment=False, columns=[]):
        try:
            conn = sqlite3.connect(self.database_path, detect_types=constants.detect_types)
            cursor = conn.cursor()
            cursor.execute(f'''PRAGMA table_info("{self.table.strip("[]")}")''')
            result = cursor.fetchall()
            if len(result) == 0:
                if (len(self.primary_keys) > 1):
                    command = f'''CREATE TABLE {self.table} ({", ".join([f'{key} integer NOT NULL' for key in self.primary_keys])},
                        CONSTRAINT pk_tableId PRIMARY KEY ({",".join(self.primary_keys)}))'''
                else:
                    type_and_rest = f'integer NOT NULL PRIMARY KEY{" AUTOINCREMENT" if auto_increment else ""}'
                    command = f'''CREATE TABLE {self.table} ({self.primary_keys[0]} {type_and_rest})'''

                cursor.execute(command)
                    
            conn.commit()
        
            for column in columns:
                cursor.execute(f"pragma table_info('{self.table.strip('[]')}')")
                curr_columns = cursor.fetchall()
                curr_columns = [x[1] for x in curr_columns]
                if (not column.name in curr_columns):
                    self._add_column(column.name, column.type, column.specialities)
                    
            conn.close()
            
        except Exception as er:
            logging.exception(er)

    async def _drop_table(self):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f'DROP TABLE {self.table}')
                
            await conn.commit()

    async def _check_primary_key(self, primary_key):
        """Checks if primary key is valid. And fixes it if needed."""
        assert primary_key != None
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        assert len(primary_key) == len(self.primary_keys)

        return primary_key
        

    async def _get_columns(self):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f'SELECT name FROM pragma_table_info("{self.table}")')
                columns = await cursor.fetchall()
                return [x[0] for x in columns]

    def _add_column(self, column_name, data_type, specialities: t.Optional[list]):
        conn = sqlite3.connect(self.database_path, detect_types=constants.detect_types)
        cursor = conn.cursor()
        command = f'ALTER TABLE {self.table} ADD {column_name} {data_type}'
        if (specialities and len(specialities) > 0):
            command += " "
            command += " ".join(specialities)
        cursor.execute(command)

        conn.commit()
        conn.close()



    async def _get(self, select_query: query.SelectQuery):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(*select_query.get_query())
                return await cursor.fetchall()

    async def _update(self, update_query: query.UpdateQuery):
        if update_query.length() == 0:
            return

        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(*update_query.get_query())

            await conn.commit()

    async def _insert(self, insert_query: query.InsertQuery):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(*insert_query.get_query())
                
            await conn.commit()

    async def _delete(self, delete_query: query.DeleteQuery):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(*delete_query.get_query())

            await conn.commit()
    
    async def get_column_list(self) -> t.List[str]:
        return await self._get_columns()

    async def check_column(self, column: Column):
        return await self.add_column(column)
        
    async def add_column(self, column: Column):
        if (not column.name in await self.get_column_list()):
            return self._add_column(column.name, column.type, column.specialities)

    async def get_or_create(self, primary_key):
        primary_key = await self._check_primary_key(primary_key)

        data = await self.get_with(primary_key)
        if not data:
            await self.set(primary_key)
            data = await self.get_with(primary_key)
        return data

    async def get_with(self, primary_key, select_query: query.SelectQuery=None) -> t.Optional[sqlite3.Row]:
        primary_key = await self._check_primary_key(primary_key)

        if not select_query:
            select_query = query.SelectQuery()

        for k, v in zip(self.primary_keys, primary_key):
            if not select_query.check_where(k,v):
                select_query.add_where(equals={k:v})

        if not select_query.table:
            select_query.table = self.table
                
        select_query.set_limit(1)

        result_row = await self._get(select_query)
        return result_row[0] if len(result_row) != 0 else None

    async def get_one(self, select_query: query.SelectQuery) -> sqlite3.Row:
        select_query.set_limit(1)
        result = await self.get(select_query)
        return result[0] if result else None
        
    async def get(self, select_query: query.SelectQuery) -> t.List[sqlite3.Row]:
        if not select_query.table:
            select_query.table = self.table

        return await self._get(select_query)

    async def get_column(self, column_name) -> t.List[t.Any]:
        column_rows = await self._get(query.SelectQuery(table=self.table, columns=[column_name]))
        return [x[0] for x in column_rows]

    async def get_all(self) -> t.List[sqlite3.Row]:
        return await self._get(query.SelectQuery(table=self.table))

    async def set(self, primary_key=None, set_query: query.SetQuery=None):
        if not set_query:
            set_query = query.SetQuery()

        if not set_query.table:
            set_query.table = self.table

        if not primary_key:
            assert set_query.length() != 0
            if isinstance(set_query, query.InsertQuery):
                if isinstance(set_query, query.SetQuery):
                    set_query = set_query.get_insert_query()
                await self._insert(set_query)
                
            elif isinstance(set_query, query.UpdateQuery):
                await self._update(set_query)
            return

        primary_key = await self._check_primary_key(primary_key)

        if (await self.get_with(primary_key)):
            for k, v in zip(self.primary_keys, primary_key):
                if not set_query.check_where(k, v):
                    set_query.add_where(equals={k:v})

            if isinstance(set_query, query.SetQuery):
                set_query = set_query.get_update_query()
            return await self._update(set_query)

        else:
            values = set_query.get_values()
            for k, v in zip(self.primary_keys, primary_key):
                if k not in values:
                    set_query.set_values(**{k:v})

            if isinstance(set_query, query.SetQuery):
                set_query = set_query.get_insert_query()
            return await self._insert(set_query)

    async def delete(self, delete_query: query.DeleteQuery):
        """All entries according to information will be deleted."""
        if not delete_query.table:
            delete_query.table = self.table

        return await self._delete(delete_query)

    async def copy_to_table_on_another_db(self, db_name: str, target_table_name: str):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f'ATTACH DATABASE "{get_datafile_path(db_name)}" AS new_db')
                await cursor.execute(f'INSERT INTO new_db.{target_table_name} SELECT * FROM {self.table};')

            await conn.commit()

    async def list_tables(self):
        async with asqlite.connect(self.database_path, detect_types=constants.detect_types) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables_column = await cursor.fetchall()
                return [x[0] for x in tables_column if not (x[0].startswith("sqlite_") or x[0].startswith("_"))]

    async def write_to_file(self, file_name: str, select_query: query.SelectQuery=None) -> str:
        if not select_query:
            select_query = query.SelectQuery()

        if (not file_name.endswith(".csv")):
            file_name += ".csv"
        
        data = await self.get(select_query)
        filePath = get_datafile_path(file_name)
        with open(filePath, "w", encoding="utf-8") as file:
            if len(data) > 0:
                file.write("; ".join(map(str, data[0].keys())) + "\n")
                
            for row in data:
                file.write(";".join(map(str, row)) + "\n")

        return filePath