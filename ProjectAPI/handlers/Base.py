from aiohttp.web import View
from sqlalchemy import create_engine, text, select, update
import json
from handlers.tables import DATA_BASE_ACCESS_STR, history_table, current_table
import uuid


# общий родительский класс для обработчиков
class BaseView(View):

    @staticmethod
    def get_engine_43():
        '''Возвращает движок бд для подключения в обработчике'''
        engine = create_engine(DATA_BASE_ACCESS_STR)
        return engine

    async def data_read_43(self):
        '''Фунция для чтения тела запроса и его перевода json в словарь'''
        body_request = b''
        async for line in self.request.content:
            body_request += line
        # Декодируя строку, нужно заменить ' на ", иначе не переведётся в dict
        dct = json.loads(body_request.decode('utf-8').replace("\'", "\""))
        return dct

    @staticmethod
    def block(conn):
        '''Функция вешает рекомендательную блоикровку. Полезно для импорта и удаления'''
        conn.execute(text('SELECT pg_advisory_xact_lock(1)'))
        return

    @staticmethod
    def check_id_date_43(id, date, conn):
        '''Функция проверяет наличие пары id-updateDate в history_table'''
        stmt = ( select(history_table.c.id).
                where(history_table.c.updatedate == date, history_table.c.id == id)
                ).exists()
        stmt = select(stmt)
        for data in conn.execute(stmt):
            alr_exists = data[0]
        return alr_exists

    def journey_to_the_root(
            self, conn, id, add_price, add_quantity, date=None, delete=False
    ):
        '''
        Функция для изменения параметров общей цены и количества различных товаров в категории. Работает и для импорта,
        и для удаления. Используется либо только для увилечения, либо только для уменьшения этих показателей
        '''
        # если id None, то мы изменили всю цепочку
        if id is None:
            return

        # выгружаем старую строчку
        stmt = select(current_table).where(current_table.c.id == id)
        for row in conn.execute(stmt):
            old_item = row

        # извлекаем нужные переменные
        old_price = old_item[3]
        old_quantity = old_item[5]
        parent = old_item[2]

        # для удаления не предусмотрена дата, используем старую
        # это приведёт к неуникальности пар id-updateDate в таблице history
        if date is None:
            date = old_item[6]

        # обновляем данные в таблице current
        stmt = update(current_table).where(current_table.c.id == id).values(
            price=old_price+add_price, product_quantity=old_quantity+add_quantity, updatedate=date
        )
        conn.execute(stmt)

        # возьмём их из таблицы обратно в виде row для иницилизация строчки в таблице history
        stmt = select(current_table).where(current_table.c.id == id)
        for row in conn.execute(stmt):
            curr_item = row

        # работаем с таблицей history
        # для пост запроса: если пара айди-дейт существует, то обновим, если нет (для делит всегда), то создадим строку
        if self.check_id_date_43(id, date, conn) and (not delete):
            stmt = update(history_table).where(
                history_table.c.id == id, history_table.c.updatedate == date
            ).values(
                price=curr_item[3], product_quantity=curr_item[5]
            )
        else:
            stmt = history_table.insert().values(
                id=curr_item[0],
                name=curr_item[1],
                parentid=curr_item[2],
                price=curr_item[3],
                type=curr_item[4],
                product_quantity=curr_item[5],
                updatedate=curr_item[6],
                generated_id=uuid.uuid4()
            )

        conn.execute(stmt)

        # вызываем эту же функцию у родителя
        self.journey_to_the_root(
            conn, parent, add_price, add_quantity, date=date, delete=delete
         )
        return

    @staticmethod
    def show_current(conn):
        '''Показывает содержимое таблицы current_table'''
        print()
        print()
        print('Current_table')
        stmt = select(current_table)
        for row in conn.execute(stmt):
            print(row)
        return

    @staticmethod
    def show_history(conn):
        '''Показывает содержимое таблицы history_table'''
        print()
        print()
        print('History_table')
        stmt = select(history_table)
        for row in conn.execute(stmt):
            print(row)
        return

    @staticmethod
    def check_id_43(id, conn):
        '''Функция проверяет, есть ли id в current_table'''
        stmt = (select(current_table.c.id).
                where(current_table.c.id == id)
                ).exists()
        stmt = select(stmt)
        for row in conn.execute(stmt):
            id_exists = row[0]
        return id_exists

    @staticmethod
    def date_to_str_43(date):
        '''Функция переводит объект datetime в str по указанному правилу для iso формата'''
        # время кратно секундам, поэтому микросекундны не нужны, а вот
        # FIXME часовой пояс игнорится
        date_str = date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return date_str

    def make_item_dict_43(self, row):
        # если количество товаров в категории равно нулю, то цена всей категории -> None
        if row[5] == 0:
            price = None
        # если в категории есть товары, то цена есть среднее
        else:
            price = row[3] // row[5]

        # переводим дату в строковый формат
        date = self.date_to_str_43(row[6])

        parent_id = str(row[2])
        if parent_id == 'None':
            parent_id = None

        # формируем словарь
        dct = {
            'id': str(row[0]),
            'name': row[1],
            'date': date,
            'parentId': parent_id,
            'price': price,
            'type': row[4]
        }
        return dct
