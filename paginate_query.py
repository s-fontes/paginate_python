from sqlalchemy.orm.query import Query
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import desc, asc, or_
from pydantic import BaseModel


class PaginatedData(BaseModel):
    """
    Default class for paginated response.
    """

    total_elements: int
    filtered_elements: int
    results: list


class PaginateQuery:
    """
    Class to filter, sort and paginate a query and return the results.
    """

    def __init__(self):
        self._query = None
        self._size = None
        self._page = None
        self._search = None
        self._order_by = None
        self._order = None

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, value):
        self._query = value

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, value):
        self._size = value

    @property
    def page(self):
        return self._page

    @page.setter
    def page(self, value):
        self._page = value

    @property
    def search(self):
        return self._search

    @search.setter
    def search(self, value):
        self._search = value

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        self._order_by = value

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        self._order = value

    def results(self) -> PaginatedData:
        """
        Method that performs the filtering, sorting and pagination of a query.

        :return: Object with total elements, total filtered elements and results.
        """
        query, total, filtered = search_in_cols(query=self._query, search=self._search)
        query = order_by_col(query=query, order_by=self._order_by, order=self._order)
        query = paginate(query=query, size=self._size, page=self._page)

        return PaginatedData(
            total_elements=total,
            filtered_elements=filtered,
            results=query.all()
        )


def find_col_object(col_name: str, query: Query) -> InstrumentedAttribute:
    """
    Function that returns the object of a query column based on its name.

    :param col_name: column name.
    :param query: Query type object.
    :return: Object of type InstrumentedAttribute.
    """
    for col_data in query.column_descriptions:
        if col_data['name'] == col_name:
            return col_data['expr']


def search_in_cols(search: str, query: Query) -> tuple:
    """
    Function that searches for the occurrence of one or more words in all columns of a query.

    :param search: string of words separated by spaces.
    :param query: Query type object.
    :return: tuple(Query type object, total elements, total filtered elements).
    """

    query_total = query

    list_of_col_objects = list(map(lambda col_data: col_data['expr'], query.column_descriptions))

    if search:
        for word in search.split(" "):
            search_args = list(map(lambda col: col.like('%%' + word + '%%'), list_of_col_objects))

            query = query.filter(or_(*search_args))

    return query, query_total.count(), query.count()


def order_by_col(query: Query, order_by: str, order: str) -> Query:
    """
    Function to sort a query based on a column and an order.

    :param query: Query type object.
    :param order_by: column name for sorting.
    :param order: ascending(asc) or descending(desc) order.
    :return: Query type object.
    """
    if order_by and order == "desc":
        query = query.order_by(
            desc(find_col_object(order_by, query))
        )

    elif order_by and order == "asc":
        query = query.order_by(
            asc(find_col_object(order_by, query))
        )

    return query


def paginate(query: Query, size: int, page: int) -> Query:
    """
    Function to paginate a query.

    :param query: Query type object.
    :param size: page size.
    :param page: page number starting from 0.
    :return: Query type object.
    """
    if size:
        query = query.limit(
            size
        ).offset(
            page * size
        )

    return query
