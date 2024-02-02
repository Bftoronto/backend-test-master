from contextlib import asynccontextmanager
from functools import partial
from typing import Union

import strawberry
from strawberry.types import Info
from fastapi import FastAPI
from strawberry.fastapi import BaseContext, GraphQLRouter
from databases import Database

from settings import Settings


class Context(BaseContext):
    db: Database

    def __init__(
        self,
        db: Database,
    ) -> None:
        self.db = db



@strawberry.type
class Author:
    name: str


@strawberry.type
class Book:
    title: str
    author: Author


@strawberry.type
class Query:

    # @strawberry.field
    # async def books(
    #     self,
    #     info: Info[Context, None],
    #     author_ids: list[int] | None = None,
    #     search: str | None = None,
    #     limit: int | None = None,
    # ) -> list[Book]:
    #     # TODO:
    #     # Do NOT use dataloaders
    #     await info.context.db.execute('select 1')
    #     return []

    @strawberry.field
    async def books(
            self,
            info: Info[Context, None],
            author_ids: list[int] | None = None,
            search: str | None = None,
            limit: int | None = None,
    ) -> list[Book]:
        query_conditions = []
        query_params: dict[str, Union[list[int], str, int]] = {}
        # query_params = {}

        if author_ids:
            query_conditions.append("author_id = ANY(:author_ids)")
            query_params["author_ids"] = author_ids
        if search:
            query_conditions.append("title LIKE :search")
            query_params["search"] = f"%{search}%"

        base_query = "SELECT books.title, authors.name FROM books JOIN authors ON books.author_id = authors.id"
        if query_conditions:
            base_query += " WHERE " + " AND ".join(query_conditions)
        if limit:
            base_query += " LIMIT :limit"
            query_params["limit"] = limit

        results = await info.context.db.fetch_all(query=base_query, values=query_params)
        return [Book(title=result[0], author=Author(name=result[1])) for result in results]



CONN_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"
settings = Settings()  # type: ignore
db = Database(
    CONN_TEMPLATE.format(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        port=settings.DB_PORT,
        host=settings.DB_SERVER,
        name=settings.DB_NAME,
    ),
)

@asynccontextmanager
async def lifespan(
    app: FastAPI,
    db: Database,
):
    async with db:
        yield

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(  # type: ignore
    schema,
    context_getter=partial(Context, db),
)

app = FastAPI(lifespan=partial(lifespan, db=db))
app.include_router(graphql_app, prefix="/graphql")