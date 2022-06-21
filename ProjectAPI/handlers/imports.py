import uuid
from handlers.Base import BaseView
from aiohttp import web
import json
from .ZEFO import EXAMPLE
from handlers.Schemas import Import
from marshmallow import ValidationError
from sqlalchemy import exists, select, update
from handlers.tables import current_table, history_table


class Post(BaseView):
    URL = '/imports'

    @staticmethod
    def item_list_check_43(item_list):
        '''
        Метод проверяет неповторяемость UUID в запросе, отсутствие цены у категории и неотрицатнльной цены у товара
        '''
        unique_id = set()
        for item in item_list:
            unique_id.add(item['id'])

            # проверка на отсутсвие цены в категории
            if (item['type'] == 'CATEGORY') and ('price' in item) and (item['price'] is not None):
                return ''

            # проверка на наличие неотрицательной цены у offer
            if (item['type'] == 'OFFER') and (item.get('price', -1) < 0):
                return ''

        # в одном запросе id не должны повторяться
        if len(unique_id) != len(item_list):
            return ''
        return 'correct'

    @staticmethod
    def items_sort_43(item_list):
        '''
        Упорядочивает список товаров: сначала категории, потом товары. Если в импорте идёт родительская и дочерняя
        категории, то родитель будет идти первее ребёнка
        '''
        def part_cat_order(id, id_dict, item_list):
            '''
            Функция возвращает список индексов для списка категорий от последнего достпуного родителя в импорте
            до отправленного элемента
            '''
            index = id_dict[id]
            parent_id = item_list[index]['parentId']
            if (parent_id is None) or (parent_id not in id_dict):
                return str(index)
            return part_cat_order(parent_id, id_dict, item_list) + str(index)

        item_list = sorted(item_list, key=lambda x: x['type'])

        # найдём количество category в списке и создадим словарь id category вместе со множеством родителей
        cat_len = 0
        id_dict = dict()
        parent_id_set = set()
        while cat_len < len(item_list) and item_list[cat_len]['type'] == 'CATEGORY':
            id_dict[item_list[cat_len]['id']] = cat_len
            cat_len += 1

        # получаем порядок расположение категорий (индексы первично отсортированного списка)
        cat_order = []
        while len(id_dict) != 0:
            id, index = id_dict.popitem()
            id_dict[id] = index
            indexes_str = part_cat_order(id, id_dict, item_list)
            indexes = list(map(int, list(indexes_str)))
            for index in indexes:
                index = int(index)
                id = item_list[index]['id']
                id_dict.pop(id, '')
            cat_order.extend(indexes)

        # собираем финальный список
        ordered_list = []
        for i in range(cat_len):
            ordered_list.append(item_list[cat_order[i]])
        ordered_list.extend(item_list[cat_len:])
        return ordered_list

    def _cat_prepare_43(self, item, date, conn):
        '''Внутренняя функция для предобработки обновления категорий'''

        # !!! в данной реализации, категории будут считаться обновлёнными,
        # !!! если у них удалился или прибавился сын, имеющий внутри товары

        id = item['id']
        parent = item['parentId']

        # выгрузим сначала информацию о старой строчке
        stmt = select(current_table).where(current_table.c.id == id)
        for row in conn.execute(stmt):
            old_item = row

        # суммарные цена и количество товаров в категории при её обновлении в запросе не могут измениться,
        # но они меняются для старого и нового родителей
        price = old_item[3]  # порядок столбцов смотреть в tables
        quantity = old_item[5]
        old_parent = old_item[2]

        # обновим в строчке БД все возможные изменяемые свойства
        stmt = update(current_table).where(
            current_table.c.id == id
        ).values(name=item['name'], parentid=parent, updatedate=date)
        conn.execute(stmt)

        # обновление в таблице history не требуется, так как в этом элементе не меняется средняя цена

        # если родитель не изменился, то родительские категории не требуют обновления (цена и количество не поменяются)
        if old_parent == parent:
            return

        # если  поле parentId изменилось, то есть ещё один случай,
        # когда изменять самих родителей не требуется: товары в категории отсутствуют
        elif quantity == 0:
            return

        # в остальных случаях спокойно запускаем апдейты родителей к корню
        else:
            # к новому родителю идёт прибавка
            self.journey_to_the_root(
                conn, parent,
                price, quantity, date= date
            )
            # у старого отнимаются значения цены и количества
            diff_pr = - price
            diff_q = - quantity
            self.journey_to_the_root(
                conn, old_parent,
                diff_pr, diff_q, date=date
            )
        return

    def _off_prepare_43(self, item, date, conn):
        '''Функция для предобработки обновлённого товара'''

        id = item['id']
        new_parent = item['parentId']

        # выгрузим сначала информацию о старой строчке
        stmt = select(current_table).where(current_table.c.id == id)
        for row in conn.execute(stmt):
            old_item = row

        # берём из неё старой строки необходимые данные и новую из свежей инфы
        old_price = old_item[3]
        old_parent = old_item[2]
        new_price = item['price']

        # обновляем строку
        stmt = update(current_table).where(
            current_table.c.id == id
        ).values(name=item['name'], parentid=new_parent, updatedate=date, price=new_price)
        conn.execute(stmt)

        # товар изменился, значит добавим строчку в history_table
        stmt = history_table.insert().values(
                        id=id,
                        name=item['name'],
                        parentid=new_parent,
                        price=new_price,
                        type=item['type'],
                        product_quantity=1,
                        updatedate=date,
                        generated_id=uuid.uuid4()
                    )
        conn.execute(stmt)

        # если родич не поменялся, то отсылаем разницу цен
        if old_parent == new_parent:
            diff = new_price - old_price
            self.journey_to_the_root(
                conn, new_parent,
                diff, 0, date=date
            )
            return

        # если изменился, то активируем две функции: уменьшение цены старого родителя (и количества)
        # и увеличение для нового
        else:
            # к новому
            self.journey_to_the_root(
                conn, new_parent,
                new_price, 1, date=date
            )
            # к старому
            diff = -old_price
            self.journey_to_the_root(
                conn, old_parent,
                diff, -1, date=date
            )
        return

    async def post(self):
        body_error = {"code": 400, "message": "Validation Failed"}
        request_body = await self.data_read_43()

        # валидируем данные с помощью схемы
        try:
            request_body = Import().load(request_body)
        except ValidationError:
            return web.Response(body=json.dumps(body_error), status=400)

        item_lst = request_body['items']
        date_time = request_body['updateDate']

        # проверка на неповоряемость UUID в запросе, отсутсивия цены у категории и неотрицательной цены у товара
        if not self.item_list_check_43(item_lst):
            return web.Response(body=json.dumps(body_error), status=400)

        # сортируем список предметов: сначала категории, потом товары
        # сортировка категорий проводится с учётом parentId так, чтобы сначала шли имеющиеся родители
        # в запросе, потом их дети
        item_lst = self.items_sort_43(item_lst)

        # FIXME асинхронный контекстный менеджер выдаёт ошибку AttributeError: __aenter__
        # FIXME инфы по решению проблемы нет, придётся делать не асинхронно
        # FIXME на уровне post и delete это не проблема, так как всё равно придётся лочить доступ, но для get запросов..
        # FIXME в идеале это нужно починить и проставить await у запросов в БД
        # подключаемся к базе данных
        # начинаем транзакцию, в случае прерывания подключения, данные не будут сохранены
        engine = self.get_engine_43()
        with engine.begin() as conn:

            # делаем рекомендательную блокировку для ликвидации состояния гонки
            self.block(conn)

            for item in item_lst:
                id = item['id']
                parent_id = item['parentId']
                curr_type = item['type']

                # проверим, что заявленный родитель есть в базе и его тип - это категория
                if parent_id is not None:
                    stmt = exists(current_table.c.id).where(
                        current_table.c.id == parent_id and current_table.c.type == 'CATEGORY'
                    ).select()
                    for row in conn.execute(stmt):
                        right_parent = row[0]
                    if not right_parent:
                        return web.Response(body=json.dumps(body_error), status=400)

                # проверим есть ли id в текущей базе
                stmt = exists(current_table.c.id).where(current_table.c.id == id).select()
                for row in conn.execute(stmt):
                    update = row[0]

                # если есть, то совершим последнюю проверку ввода данных на неизменность типа
                if update:
                    stmt = select(current_table).where(current_table.c.id == id)
                    for row in conn.execute(stmt):
                        old_type = row[4]
                    if old_type != curr_type:
                        return web.Response(body=json.dumps(body_error), status=400)

                # в логике решения задачи с сохранением суммарной цены категории и количества юнитов в ней
                # для вычисления средней цены, нужно особым способом инициализировать изменения в БД
                # если категорися новая, то просто создадим её + создадим её копию в таблицу history_table
                if (curr_type == 'CATEGORY') and (not update):
                    stmt = current_table.insert().values(
                        id=id,
                        name=item['name'],
                        parentid=parent_id,
                        price=0,
                        type=curr_type,
                        product_quantity=0,
                        updatedate=date_time
                    )
                    conn.execute(stmt)

                    stmt = history_table.insert().values(
                        id=id,
                        name=item['name'],
                        parentid=parent_id,
                        price=0,
                        type=curr_type,
                        product_quantity=0,
                        updatedate=date_time,
                        generated_id=uuid.uuid4()
                    )
                    conn.execute(stmt)

                # если категория существует, то нужно рассмотреть ещё пару случаев, прежде чем идти вниз по дереву
                # для апдейта средних цен родительских категорий
                elif curr_type == 'CATEGORY':
                    self._cat_prepare_43(item, date_time, conn)

                # добавление нового товара только увеличивает цену родительских категорий, поэтому
                # создаем новую строчку в обеих таблицах БД и запускаем изменение категорий
                if (curr_type == 'OFFER') and (not update):
                    stmt = current_table.insert().values(
                        id=id,
                        name=item['name'],
                        parentid=parent_id,
                        price=item['price'],
                        type=curr_type,
                        product_quantity=1,
                        updatedate=date_time
                    )
                    conn.execute(stmt)

                    stmt = history_table.insert().values(
                        id=id,
                        name=item['name'],
                        parentid=parent_id,
                        price=item['price'],
                        type=curr_type,
                        product_quantity=1,
                        updatedate=date_time,
                        generated_id=uuid.uuid4()
                    )
                    conn.execute(stmt)
                    self.journey_to_the_root(
                        conn, parent_id,
                        item['price'], 1, date=date_time,
                    )

                # если же апдейт, то нужно правильно вызвать
                # функции обновления родительских каталогов
                elif curr_type == 'OFFER':
                    self._off_prepare_43(item, date_time, conn)

                # снятие блокировки должно происходить автоматически после завершения транзакции
                # транзакция заавершается после выхода из контексного менеджера

                # данный алгоритм достаточно громоздкий, но его выполнение обеспечивает эффективное и верное
                # выполнение дополнительных гет-запросов в тз

        body = {"code": 200, "message": "Successful transaction"}
        return web.Response(body=json.dumps(body), status=200)
