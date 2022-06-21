from handlers.Base import BaseView
from handlers.tables import current_table
from sqlalchemy import select
from aiohttp import web
import json
from datetime import timedelta
from dateutil import parser
from dateutil.parser import ParserError


class GetSales(BaseView):
    URL = '/sales'

    async def get(self):
        # получаем информацию о дополнительных параметрах в url строке
        get_str = self.request.query_string

        # валидируем запрос
        body_error = {"code": 400, "message": "Validation Failed"}
        split_get = get_str.split(sep='=')
        if split_get[0] != 'date':
            return web.Response(body=json.dumps(body_error), status=400)
        try:
            max_date = parser.parse(split_get[1])
        except ParserError:
            return web.Response(body=json.dumps(body_error), status=400)

        # находим нижнюю границу
        min_date = max_date - timedelta(hours=24)

        # FIXME нет асинхронности
        # делаем транзакцию с блокировкой
        engine = self.get_engine_43()
        with engine.begin() as conn:
            self.block(conn)

            # создадим словарь для выгрузки
            dct = {'items': []}

            # делаем запрос к current_table
            stmt = select(current_table).where(
                current_table.c.updatedate >= min_date, current_table.c.updatedate < max_date
            )
            # каждую строку отформатируем и запишем в словарь
            for row in conn.execute(stmt):
                item = self.make_item_dict_43(row)
                dct['items'].append(item)

        # вернём полученные данные
        return web.Response(body=json.dumps(dct), status=200)


