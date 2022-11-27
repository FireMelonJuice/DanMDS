import asyncio
import aiomysql
import argparse
import pandas as pd
import numpy as np
import re
from sklearn.neighbors import KernelDensity

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

    async def get_data(self, sql):
        async with self.db_pool.acquire() as conn:
            await conn.select_db('release_db')
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                return await cursor.fetchall()
    
    async def update_data(self, sql):
        async with self.db_pool.acquire() as conn:
            await conn.select_db('release_db')
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                await conn.commit()

    def get_ft(self, df):
        X = df['Time'].loc[(df['Action'] == 'PUSH_Attack') | (df['Action'] == 'PUSH_Jump')].to_numpy()
        #X = df['Time'].loc[(df['Action'] == 'PUSH_Attack')].to_numpy()
        kde = KernelDensity(bandwidth=3)
        kde.fit(X[:, None])
        X_d = np.linspace(0, X[-1], int(X[-1]) + 1)
        data = np.exp(kde.score_samples(X_d[:, None]))
        div10 = int(len(data) / 10)
        data = data[div10:-div10]
        X_d = X_d[div10:-div10]
        data /= max(data)
        metadata = [[len(data), 0]]
        kde_df = pd.concat([pd.DataFrame(data, columns=['0']), pd.DataFrame(X_d, columns=['1'])], axis=1)

        acf_data = []
        acf_X = []
        for i in range(len(data) - 1):
            acf_data.append(pd.Series(data).autocorr(lag=i))
            acf_X.append((X_d[-1] - X_d[0]) * i / (len(data) - 1))
        div10 = int(len(acf_data) / 10)
        acf_data = acf_data[:-div10]
        acf_X = acf_X[:-div10]
        metadata.append([len(acf_data), 0])
        acf_df = pd.concat([pd.DataFrame(acf_data, columns=['0']), pd.DataFrame(acf_X, columns=['1'])], axis=1)

        strength = pd.DataFrame(abs(np.fft.fft(acf_data)[1:]), columns=['0'])
        frequency = pd.DataFrame(1 / np.fft.fftfreq(len(acf_data), 1)[1:], columns=['1'])
        ft_data = pd.concat([strength, frequency], axis=1)
        ft_data = ft_data.loc[ft_data['1'] >= 0].sort_values(by=['1'])
        metadata.append([len(ft_data), 0])
        metadata = pd.DataFrame(metadata, columns=['0', '1'])
        output = pd.concat([metadata, kde_df, acf_df, ft_data], axis=0, ignore_index=True)
        return output

    @Request.get('/UserList')
    async def send_basic_data(self, writer, data):
        sql = 'SELECT a.name, a.user_desc, a.date FROM Users AS a'
        result = await self.get_data(sql)

        str_list = []
        for data in result:
            data = list(data)
            name = data[0]
            user_desc = data[1]
            date = data[2]
            str_list.append(f'{name},{user_desc},{date}')
        csv_str = '\n'.join(str_list)

        response = self.build_response(csv_str)
        writer.write(response)

    @Request.get('/EstimateList')
    async def send_estimate_data(self, writer, data):
        sql = 'SELECT b.name, a.eval FROM Estimate AS a JOIN Users AS b ON a.user_idx = b.idx'
        result = await self.get_data(sql)

        str_list = []
        for data in result:
            data = list(data)
            name = data[0]
            user_eval = data[1]
            str_list.append(f'{name},{user_eval}')
        csv_str = '\n'.join(str_list)

        response = self.build_response(csv_str)
        writer.write(response)

    @Request.get('/SearchUser')
    async def send_search_data(self, writer, data):
        sql = f'SELECT b.name, a.map, a.action, a.time, a.x_coord, a.y_coord, a.date FROM GameLog AS a JOIN Users AS b ON a.user_idx = b.idx WHERE b.name = "{data}"'
        result = await self.get_data(sql)

        if len(result) == 0:
            response = self.build_response("")
            writer.write(response)
            return

        time_list = []
        action_list = []
        for data in result:
            data = list(data)
            action_list.append(data[2])
            time_list.append(data[3])
        df = pd.concat([pd.DataFrame(time_list, columns=['Time']), pd.DataFrame(action_list, columns=['Action'])], axis=1)
        csv_str = self.get_ft(df).to_csv(header=None, index=False)

        response = self.build_response(csv_str)
        writer.write(response)

    @Request.get('/TestData')
    async def send_basic_data(self, writer, data):
        sql = 'SELECT b.name, a.map, a.action, a.time, a.x_coord, a.y_coord, a.date FROM GameLog AS a JOIN Users AS b ON a.user_idx = b.idx WHERE b.name = "test_user"'
        result = await self.get_data(sql)

        str_list = []
        for d in result:
            d = list(d)
            action = d[2]
            time = d[3]
            x_coord = d[4]
            y_coord = d[5]
            str_list.append(f'{time},{action},{x_coord},{y_coord}')
        csv_str = '\n'.join(str_list)

        response = self.build_response(csv_str)
        writer.write(response)

    @Request.get('/HumanData')
    async def send_human_data(self, writer, data):
        sql = 'SELECT b.name, a.map, a.action, a.time, a.x_coord, a.y_coord, a.date FROM GameLog AS a JOIN Users AS b ON a.user_idx = b.idx WHERE b.name = "NOMACRO"'
        result = await self.get_data(sql)

        str_list = []
        for d in result:
            d = list(d)
            action = d[2]
            time = d[3]
            x_coord = d[4]
            y_coord = d[5]
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
            token = re.split("/", url)
            url_header = '/' + token[1]
            if len(token) > 2:
                data = token[2]
            else:
                data = None
            print(url, token)
            if url_header in Request.path_dict:
                await Request.path_dict[url_header](self, writer, data)

        writer.close()

    async def estimate_user(self, user_idx):
        print("estimate")
        sql = f'SELECT action, time FROM GameLog WHERE user_idx = {user_idx}'
        result = await self.get_data(sql)
                
        time_list = []
        action_list = []
        for data in result:
            data = list(data)
            time_list.append(data[1])
            action_list.append(data[0])

        if (len(time_list) == 0 or max(time_list) < 60.0):
            sql = f'UPDATE Estimate SET eval = 0 WHERE user_idx = {user_idx}'
            await self.update_data(sql)
            return

        df = pd.concat([pd.DataFrame(time_list, columns=['Time']), pd.DataFrame(action_list, columns=['Action'])], axis=1)
        ft_data = self.get_ft(df)
        ev = max(ft_data['0'].to_numpy()[3:])
        sql = f'UPDATE Estimate SET eval = {ev}, last_update = CURRENT_TIMESTAMP WHERE user_idx = {user_idx}'
        await self.update_data(sql)

    async def test_task(self):
        sql = 'SELECT * FROM Estimate WHERE last_update < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 20 SECOND) AND eval IS NULL'
        
        while True:
            result = await self.get_data(sql)
            for data in result:
                await self.estimate_user(data[0])
            await asyncio.sleep(10)

    async def start(self):
        self.db_pool = await aiomysql.create_pool(host='localhost', user=self.user, password=self.password, db='release_db', charset='utf8', loop=self.loop)

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        task = asyncio.create_task(await self.test_task())
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
