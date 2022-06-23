from aiohttp import web
from handlers.handlers import HANDLERS
from handlers.tables import DATA_BASE_ACCESS_STR
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime, create_engine
from sqlalchemy.dialects.postgresql import UUID
import time

# создаём подключение БД и таблицы
engine = create_engine(DATA_BASE_ACCESS_STR)    # адрес подклчения лежит в handlers.tables
metadata_obj = MetaData()

# актуальная таблица имеющихся товаров и категорий
# главный ключ - айди товара
current_table = Table(
    'current_offers',
    metadata_obj,
    Column('id', UUID(as_uuid=True), primary_key=True),
    Column('name', String, nullable=False),
    Column('parentid', UUID(as_uuid=True), nullable=True),
    Column('price', Integer, nullable=False),
    Column('type', String, nullable=False),
    Column('product_quantity', Integer, nullable=False),
    Column('updatedate', DateTime, nullable=False)
)

# таблица, содержащая историю изменений неудалённых элементов
# поля в таблице совпадают с прошлыми, кроме последнего поля с генерированным uuid (оно есть главный ключ)
history_table = Table(
    'offers_history',
    metadata_obj,
    Column('id', UUID(as_uuid=True)),
    Column('name', String, nullable=False),
    Column('parentid', UUID(as_uuid=True), nullable=True),
    Column('price', Integer, nullable=False),
    Column('type', String, nullable=False),
    Column('product_quantity', Integer, nullable=False),
    Column('updatedate', DateTime),
    Column('generated_id', UUID(as_uuid=True), primary_key=True),
)

metadata_obj.create_all(engine)

## подождём на всякий случай секунд 15, приложение же может запускаться минуту =)
#time.sleep(15)

# запускаем приложение
app = web.Application()
for handler in HANDLERS:
    app.router.add_view(handler[0], handler[1])
web.run_app(app, host='0.0.0.0', port=80)
