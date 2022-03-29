import sqlite3
import traceback
from sqlite3 import Error

tables_data = (
    {"name": "Clients",
     "data": [
         (1, "Иван"),
         (2, "Константин"),
         (3, "Дмитрий"),
         (4, "Александр")
     ]},
    {"name": "Products",
     "data": [
         (1, "Мяч", 299.99),
         (2, "Ручка", 18),
         (3, "Кружка", 159.87),
         (4, "Монитор", 18000),
         (5, "Телефон", 9999.9),
         (6, "Кофе", 159)
     ]},
    {"name": "Orders",
     "data": [
         (1, 2, 2, "Закупка 1"),
         (2, 2, 5, "Закупка 2"),
         (3, 2, 1, "Закупка 3"),
         (4, 1, 1, "Закупка 4"),
         (5, 1, 3, "Закупка 5"),
         (6, 1, 6, "Закупка 6"),
         (7, 1, 2, "Закупка 7"),
         (8, 4, 5, "Закупка 8"),
         (9, 3, 6, "Закупка 9"),
         (10, 3, 3, "Закупка 10"),
         (11, 1, 5, "Закупка 11")
     ]}
)


class DBManager:
    def __init__(self):
        self.sqlite_file = "mydb.db"
        self.conn: sqlite3.Connection = None  # Переменная соединения с БД
        self.connect()
        for table in tables_data:
            self.drop_table(table['name'])
        self.create_table_clients()
        self.create_table_products()
        self.create_table_orders()
        for table in tables_data:
            self.fill_table(table['name'], table['data'])

    def connect(self):
        """
        Подключается к БД.
        """
        try:
            self.conn = sqlite3.connect(self.sqlite_file)
            self.conn.row_factory = sqlite3.Row
            print("БД подключена.")
        except Error as e:
            print(f"connect: {e}")

    def drop_table(self, name):
        """
        Удаляет указанную таблицу из БД

        :param str name: Имя таблицы
        """
        sql = """DROP TABLE "{}" """
        try:
            self.conn.execute(sql.format(name))
        except Error as e:
            print(f"create_table: {e}")
        else:
            print(f"Таблица {name} удалена.")

    def create_table_clients(self):
        """
        Создаёт таблицу Clients
        """
        sql = """CREATE TABLE IF NOT EXISTS "Clients"(
            id_users integer PRIMARY KEY,
            user_name TEXT);"""
        try:
            self.conn.execute(sql)
        except Error as e:
            print("create_table_clients: {}".format(e))
        else:
            print("Таблица Clients создана.")

    def create_table_products(self):
        """
        Создаёт таблицу Products
        """
        sql = """CREATE TABLE IF NOT EXISTS "Products"(
            id_product integer PRIMARY KEY,
            product_name TEXT,
            price REAL);"""
        try:
            self.conn.execute(sql)
        except Error as e:
            print("create_table_products: {}".format(e))
        else:
            print("Таблица Products создана.")

    def create_table_orders(self):
        """
        Создаёт таблицу Orders
        """
        sql = """CREATE TABLE IF NOT EXISTS "Orders"(
            id_order integer PRIMARY KEY,
            id_users,
            id_product,
            order_name TEXT,
            FOREIGN KEY(id_users) REFERENCES "Clients"(id_users),
            FOREIGN KEY(id_product) REFERENCES "Products"(id_product));"""
        try:
            self.conn.execute(sql)
        except Error as e:
            print("create_table_orders: {}".format(e))
        else:
            print("Таблица Orders создана.")

    def fill_table(self, table_name, data):
        """
        Заполняет указанную таблицу данными.

        :param str table_name: Имя таблицы
        :param list[tuple] data: Список кортежей с данными
        :return:
        """
        sql = """INSERT INTO "{}" VALUES {};"""

        try:
            cur = self.conn.cursor()
            for each in data:
                cur.execute(sql.format(table_name, each))
            self.conn.commit()

        except Error as e:
            print(f"fill_table: {e}")
        else:
            print(f"Вставлены новые данные в таблицу {table_name}.")

    def select_clients_with_sum_of_orders(self):
        """
        # Выполняет запрос: 1) Список клиентов с общей суммой их покупок
        :return: Список строк
        :rtype: list[sqlite3.Row]
        """
        ret = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT user_name AS "Клиент", SUM(price) AS "Общая сумма покупок"
                from "Orders"
                JOIN "Clients" ON "Orders".id_users = "Clients".id_users
                JOIN "Products" ON "Orders".id_product = "Products".id_product
                GROUP BY user_name
                """
            )
            ret = cursor.fetchall()
        except Error as e:
            print(f"select_clients_with_sum_of_orders: {e}")
        except:
            print(f"select_clients_with_sum_of_orders: {traceback.format_exc()}")
        return ret

    def select_clients_with_phones(self):
        """
        # Выполняет запрос: 2) Список клиентов, которые купили телефон
        :return: Список строк
        :rtype: list[sqlite3.Row]
        """
        ret = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT user_name AS "Клиент"
                from "Orders"
                JOIN "Clients" ON "Orders".id_users = "Clients".id_users
                JOIN "Products" ON "Orders".id_product = "Products".id_product
                WHERE "Products".product_name = "Телефон"
                GROUP BY user_name  --Исключаем повторения
                """
            )
            ret = cursor.fetchall()
        except Error as e:
            print(f"select_clients_with_phones: {e}")
        except:
            print(f"select_clients_with_sum_of_orders: {traceback.format_exc()}")
        return ret

    def select_num_of_each_product_orders(self):
        """
        # Выполняет запрос: 3) Список товаров с количеством их заказов
        :return: Список строк
        :rtype: list[sqlite3.Row]
        """
        ret = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT product_name AS "Товар", COUNT(id_order) AS "Количество заказов"
                from "Orders"
                JOIN "Products" ON "Orders".id_product = "Products".id_product
                GROUP BY product_name
                """
            )
            ret = cursor.fetchall()
        except Error as e:
            print(f"select_num_of_each_product_orders: {e}")
        except:
            print(f"select_clients_with_sum_of_orders: {traceback.format_exc()}")
        return ret

    def disconnect(self):
        try:
            self.conn.close()
            print("БД отключена.")
        except Error as e:
            print(f"disconnect: {e}")


if __name__ == '__main__':
    a = DBManager()

    print()
    print("Список клиентов с общей суммой их покупок:")
    for row in a.select_clients_with_sum_of_orders():
        print(dict(row))
    print()

    print("Список клиентов, которые купили телефон:")
    for row in a.select_clients_with_phones():
        print(dict(row))
    print()

    print("Список товаров с количеством их заказов:")
    for row in a.select_num_of_each_product_orders():
        print(dict(row))
    print()

    a.disconnect()
