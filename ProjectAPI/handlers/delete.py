from .Base import BaseView
from uuid import UUID
from aiohttp import web
import json
from sqlalchemy import select, delete
from .tables import current_table, history_table


class Delete(BaseView):
    URL = '/delete/{id}'

    def _delete_from_everywhere(self, id, conn):
        '''
        Удаляет элемент с айди из таблиц current и history.
        Также анигилирует своих детей в этих же таблицах
        '''
        # удаляем из таблицы current
        stmt = delete(current_table).where(current_table.c.id == id)
        conn.execute(stmt)

        # удаляем из таблицы history
        stmt = delete(history_table).where(history_table.c.id == id)
        conn.execute(stmt)

        # ликвидируем детей дальше
        stmt = select(current_table).where(current_table.c.parentid == id)
        for row in conn.execute(stmt):
            self._delete_from_everywhere(row[0], conn)
        return

    async def delete(self):
        body_error = {"code": 400, "message": "Validation Failed"}

        # получим строковое айди на удаление и преобразуем его в uuid (если формат правильный)
        delete_id = self.request.path_qs.replace('/delete/', '')
        try:
            delete_id = UUID(delete_id)
        except ValueError:
            return web.Response(body=json.dumps(body_error), status=400)

        # FIXME не работает асинхронный менеджер
        # начинаем транзакцию
        engine = self.get_engine_43()
        with engine.begin() as conn:

            # вешаем рекомендательную блокировку
            self.block(conn)

            #  проверим, есть ли элемент в current таблице
            id_exists = self.check_id_43(delete_id, conn)

            # если не существует, то отвечаем, что id не найден
            if not id_exists:
                body_error = {"code": 404, "message": "Item not found"}
                return web.Response(body=json.dumps(body_error), status=404)

            # выгрузим информацию из current_table о переданном id
            stmt = select(current_table).where(current_table.c.id == delete_id)
            for row in conn.execute(stmt):
                delete_item = row

            # извлечём из неё самое лучшее
            price = delete_item[3]
            parent = delete_item[2]
            quantity = delete_item[5]

            # из-за удаления предмета средние цены родительских каталогов изменятся
            # запустим функцию, которая изменит суммарную цену и количество товаров в current таблице
            self.journey_to_the_root(
                conn, parent, -price, -quantity, delete=True
            )

            # теперь нужно приступить к удалению элемента и его детей, причём в обеих таблицах
            self._delete_from_everywhere(delete_id, conn)

            # снятие блокировки должно происходить автоматически после завершения транзакции
            # транзакция заавершается после выхода из контексного менеджера

        body = {"code": 200, "message": "Item was deleted successfully"}
        return web.Response(body=json.dumps(body), status=200)
