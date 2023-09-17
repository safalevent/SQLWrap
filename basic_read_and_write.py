import asyncio
from SQLWrap import *

dataTable = Table(
    "people",
    "id",
    columns= [
        Column("name", "TEXT"),
        Column("surname", "TEXT"),
    ],
    database="test.db"
)

async def create_data():
    query = SetQuery({"id": 1, "name": "Safa", "surname": "Levent"})
    await dataTable.set(set_query=query)

async def update_data():
    query = SetQuery({"name": "Definitely Not", "surname": "Safa"})
    await dataTable.set(primary_key=1, set_query=query)

async def read_data():
    query = SelectQuery().add_where(equals={"surname": "Safa"})
    return await dataTable.get(query)

async def delete_data():
    query = DeleteQuery().add_where(equals={"surname": "Safa"})
    await dataTable.delete(query)

def print_table(table):
    for row in table:
        for entry in row:
            print(entry, end="\t")
            
        print()

async def main():
    await create_data()
    await update_data()
    print_table(await read_data())
    await delete_data()
    print(await read_data())

asyncio.run(main())