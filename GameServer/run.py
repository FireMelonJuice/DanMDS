import socket
import asyncio
import re
import pymysql


class MacroServer():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.loop = None
        self.db_connection = None

    async def handle_client(self, reader, writer):
        print("Client was connected")
        cursor = self.db_connection.cursor()

        data = (await reader.readuntil(b';')).decode('utf8')
        print(data)
        token = re.split(r'[,]', data[:-1])
        print(token)
        name = token[0]
        game_map = token[1]
        desc = token[2]

        sql = f'INSERT INTO Users(name, user_desc) VALUES ("{name}", "{desc}")'
        cursor.execute(sql)
        self.db_connection.commit()

        sql = 'SELECT idx FROM Users WHERE name = %s'
        cursor.execute(sql, name)
        result = cursor.fetchall()
        user_idx = result[0][0]
        print(user_idx)

        sql = f'INSERT INTO Estimate(user_idx) VALUES ({user_idx})'
        cursor.execute(sql)
        self.db_connection.commit()

        try:
            while True:
                data = (await reader.readuntil(b';')).decode('utf8')
                if data == "":
                    break

                print(data)
                token = re.split(r'[, ]', data[:-1])
                sql = f'INSERT INTO GameLog(user_idx, map, action, time, x_coord, y_coord) VALUES ({user_idx}, {game_map}, "{token[1]}", {token[0]}, {token[2]}, {token[3]})'
                cursor.execute(sql)
                sql = f'UPDATE Estimate SET last_update = CURRENT_TIMESTAMP WHERE user_idx = {user_idx}'
                cursor.execute(sql)
                self.db_connection.commit()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            print("socket closed")

    def run(self):
        async def runner():
            async with self:
                await self.start()

        try:
            asyncio.run(runner())
        except KeyboardInterrupt:
            pass
        finally:
            if self.db_connection is not None:
                self.db_connection.close()


    async def start(self):
        self.db_connection = pymysql.connect(host='localhost', user=USER, password=pw, db='release_db', charset='utf8')

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()

    async def __aenter__(self):
        self.loop = asyncio.get_running_loop()

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

if __name__ == "__main__":
    macro_server = MacroServer(HOST, PORT)
    macro_server.run()
