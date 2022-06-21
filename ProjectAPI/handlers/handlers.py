from handlers.imports import Post
from handlers.delete import Delete
from handlers.get_nodes import GetNodes
from handlers.get_sales import GetSales
from handlers.get_statistics import GetStatistics


HANDLERS = (
    (Post.URL, Post), (Delete.URL, Delete), (GetNodes.URL, GetNodes),
    (GetSales.URL, GetSales), (GetStatistics.URL, GetStatistics)
)
