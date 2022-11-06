import asyncio
import aiomysql
import argparse

class Request():
    path_dict = {}

    @staticmethod
    def get(path):
        def inner(func):
            Request.path_dict[path] = func
            return func
        return inner

class DataServer():
    def __init__(self, host, port, user, password, debug, loop=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.loop = loop
        self.debug = debug
        self.db_pool = None

    @Request.get('/TestData')
    async def send_basic_data(self, writer):
        sql = 'SELECT b.name, a.map, a.action, a.time, a.x_coord, a.y_coord, a.date FROM GameLog AS a JOIN Users AS b ON a.user_idx = b.idx WHERE b.name = "test_user"'

        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                result = await cursor.fetchall()

                str_list = []
                for data in result:
                    data = list(data)
                    action = data[2]
                    time = data[3]
                    x_coord = data[4]
                    y_coord = data[5]
                    str_list.append(f'{time},{action},{x_coord},{y_coord}')
                csv_str = '\n'.join(str_list)
        response = self.build_response(csv_str)
        writer.write(response)

    @Request.get('/HumanData')
    async def send_human_data(self, writer):
        sql = 'SELECT b.name, a.map, a.action, a.time, a.x_coord, a.y_coord, a.date FROM GameLog AS a JOIN Users AS b ON a.user_idx = b.idx WHERE b.name = "NOMACRO"'

        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                result = await cursor.fetchall()

                str_list = []
                for data in result:
                    data = list(data)
                    action = data[2]
                    time = data[3]
                    x_coord = data[4]
                    y_coord = data[5]
                    str_list.append(f'{time},{action},{x_coord},{y_coord}')
                csv_str = '\n'.join(str_list)
        response = self.build_response(csv_str)
        writer.write(response)

    async def handle_client(self, reader, writer):
        print("Client was connected")
        request = ''
        while True:
            chunk = (await reader.read(1024)).decode('utf8')
            request += chunk
            if len(chunk) < 1024:
                break
        method, url = self.parse_request(request)

        if method == 'GET':
            if url in Request.path_dict:
                await Request.path_dict[url](self, writer)

        writer.close()

    async def start(self):
        self.db_pool = await aiomysql.create_pool(host='localhost', user=self.user, password=self.password, db='release_db', charset='utf8', loop=self.loop)

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()

    def run(self):
        try:
            asyncio.run(self.start())
            self.loop = asyncio.get_running_loop()
        except KeyboardInterrupt:
            pass
        finally:
            pass

    def parse_request(self, request):
        request, _ = request.split('\r\n\r\n')
        lines = request.split('\r\n')
        method, url, _ = lines[0].split(' ')
        return method, url

    def build_response(self, data):
        response = 'HTTP/1.1 {status} {status_msg}\r\nContent-Type: text/csv; charset=UTF-8\r\nContent-Encoding: UTF-8\r\nAccept-Ranges: bytes\r\nConnection: closed\r\n\r\n{data}'

        return response.format(status=200, status_msg='OK', data=data).encode('utf8')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='server host')
    parser.add_argument('--port', help='server port')
    parser.add_argument('--user', help='DB user name')
    parser.add_argument('--password', help='DB user password')
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    data_server = DataServer(args.host, args.port, args.user, args.password, args.debug)
    data_server.run()
