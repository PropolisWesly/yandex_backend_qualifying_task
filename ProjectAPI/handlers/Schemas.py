from marshmallow import Schema, fields,  validate


# схема для элемента в импорте, здесь происходит несколько проверок формата, но не все
class Items(Schema):
    id = fields.UUID(required=True)
    name = fields.Str(allow_none=False, required=True)
    parentId = fields.UUID(allow_none=True, required=True)
    price = fields.Int(strict=True, validate=validate.Range(min=0), allow_none=True)
    type = fields.Str(required=True, validate=validate.OneOf(('OFFER', 'CATEGORY')))


# схема для входяшего импорта
class Import(Schema):
    items = fields.List(fields.Nested(Items()), required=True)
    updateDate = fields.DateTime(required=True)
