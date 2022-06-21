from handlers.Base import BaseView
from handlers.tables import current_table
from sqlalchemy import select
from aiohttp import web
import json
from uuid import UUID


class GetNodes(BaseView):
    URL = '/nodes/{id}'

    def _to_the_children_43(self, row, conn):
        '''Функция для создания словаря отосланного узла с его потомками'''

        # сделаем словарь для вывода
        item_dict = self.make_item_dict_43(row)

        # если это категория, то ищем детей
        if item_dict['type'] == 'CATEGORY':
            item_dict['children'] = []

            # ищем детей
            item_uuid = UUID(item_dict['id'])
            stmt = select(current_table).where(current_table.c.parentid == item_uuid)

            # если есть дети, то записываем их и ищем их детей
            for row in conn.execute(stmt):
                item_dict['children'].append(self._to_the_children_43(row, conn))
        else:
            item_dict['children'] = None

        # если это товар, то вернём его словарь
        return item_dict

    async def get(self):
        # получим строковое айди на поиск элемента и преобразуем его в uuid (если формат правильный)
        get_id = self.request.path_qs.replace('/nodes/', '')
        try:
            get_id = UUID(get_id)
        except ValueError:
            body_error = {"code": 400, "message": "Validation Failed"}
            return web.Response(body=json.dumps(body_error), status=400)

        # FIXME не работает асинхронный менеджер
        # запускаем транзакцию
        engine = self.get_engine_43()
        with engine.begin() as conn:

            # FIXME блокировка здесь не нужна, но раз уж не получилось сделать всё асинхронно, то она точно не нужна
            # FIXME  но я повешу для подстраховки
            self.block(conn)

            #  проверим, есть ли элемент в current таблице
            id_exists = self.check_id_43(get_id, conn)

            # если не существует, то отвечаем, что id не найден
            if not id_exists:
                body_error = {"code": 404, "message": "Item not found"}
                return web.Response(body=json.dumps(body_error), status=404)

            # выгрузим информацию из current_table о переданном id
            stmt = select(current_table).where(current_table.c.id == get_id)
            for row in conn.execute(stmt):
                curr_item = row

            # если это категория, то ищем детей
            item_dict = self.make_item_dict_43(curr_item)
            if item_dict['type'] == 'CATEGORY':
                item_dict['children'] = []

                # ищем детей
                stmt = select(current_table).where(current_table.c.parentid == get_id)

                # если есть дети, то записываем их и ищем их детей
                for row in conn.execute(stmt):
                    item_dict['children'].append(self._to_the_children_43(row, conn))
            else:
                item_dict['children'] = None

        return web.Response(body=json.dumps(item_dict), status=200)



