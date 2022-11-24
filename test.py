from fastapi import FastAPI
from pydantic import BaseModel
from pydantic.fields import Optional, Field

# Объявляем приложение и задаём ему название, которое будет отображаться в документации
app = FastAPI(title="Простые математические операции")


# Объявляем модель, которая будет валидировать данные, поступающие от пользователя
# При несовпадении данных со схемой пользователю вернётся ошибка валидации
class Add(BaseModel):
    first_number: int = Field(title='Первое слагаемое')
    second_number: Optional[int] = Field(title='Второе слагаемое')


# Объявляем модель для формирования результата
# При несовпадении данных со схемой вы получите подробный traceback :)
class Result(BaseModel):
    result: int = Field(title='Результат')


# Добавляем URL и привязываем к нему модели запроса и ответа
@app.post("/add",  response_model=Result)
async def create_item(item: Add):
    # Выполняем вычисления
    return {
        'result': item.first_number + item.second_number or 1
    }
