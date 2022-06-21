from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime, create_engine
from sqlalchemy.dialects.postgresql import UUID


# адрес для работы с postgresql
DATA_BASE_ACCESS_STR = 'postgresql+psycopg2://postgres:password@localhost:5432/'

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

