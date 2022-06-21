from handlers.Base import BaseView
from uuid import UUID
from aiohttp import web
import json
from dateutil import parser
from dateutil.parser import ParserError
from handlers.tables import history_table
from sqlalchemy import select


class GetStatistics(BaseView):
    URL = '/node/{id}/statistic'

    async def get(self):
        body_error = {"code": 400, "message": "Validation Failed"}

        # получим айди из запроса
        try:
            get_id = UUID(self.request.path_qs.split(sep='/')[2])
        except ValueError:
            return web.Response(body=json.dumps(body_error), status=400)

        # получим данные о датах
        dates = {}
        info_list = self.request.query_string.split(sep='&')
        for param in info_list:
            key_value = param.split(sep='=')
            dates[key_value[0]] = key_value[1]

        # валидируем полученные данные
        check = ['dateStart', 'dateEnd']
        for key in dates:

            # проверяем правильность ключа
            if key not in check:
                return web.Response(body=json.dumps(body_error), status=400)
            else:
                check.remove(key)

            # проверим правильность даты
            try:
                date_value = parser.parse(dates[key])
                dates[key] = date_value
            except ParserError:
                return web.Response(body=json.dumps(body_error), status=400)

        # последняя проверка на корректность данных - сравнение дат
        if dates['dateStart'] > dates['dateEnd']:
            return web.Response(body=json.dumps(body_error), status=400)

        # FIXME нет асинхрона
        # открываем транзакцию и лоим базу
        engine = self.get_engine_43()
        with engine.begin() as conn:
            self.block(conn)

            # проверим есть ли айди в базе current
            id_exists = self.check_id_43(get_id, conn)

            # если не существует, то отвечаем, что id не найден
            if not id_exists:
                body_error = {"code": 404, "message": "Item not found"}
                return web.Response(body=json.dumps(body_error), status=404)

            # выгрузим нужные данные из таблицы history
            stmt = select(history_table).where(
                history_table.c.id == get_id,
                history_table.c.updatedate >= dates['dateStart'],
                history_table.c.updatedate >= dates['dateEnd']
            ).order_by(history_table.c.updatedate)

            # добавим их в результат
            item_dict = {'items': []}
            for row in conn.execute(stmt):
                item = self.make_item_dict_43(row)
                item_dict['items'].append(item)

        # вернём полученные данные
        return web.Response(body=json.dumps(item_dict), status=200)




