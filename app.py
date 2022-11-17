import hashlib
import json
import logging
import sqlite3
from concurrent import futures
from datetime import datetime
import pymysql
from time import time
from uuid import uuid4
from flask import Flask, jsonify, request
from urllib.parse import urlparse
from werkzeug.middleware.proxy_fix import ProxyFix
import requests
import re
import redis
import sys


DIFFICULTY_COUNT = 3

class Blockchain(object):
    # 区块链初始化
    def __init__(self):
        self.chain = []
        self.currentTransaction = []  # 用于记录经矿工确认合法的交易信息 等待写入下一个新创建的区块
        self.currentRw = []
        self.nodes = set()  # 无序元素集合 用于存储区块链网络中已发现的所有节点信息
        # Create the genesis block
        self.new_block()
        self.neighbor = []
        # try:
        #     self.conn = pymysql.connect(host='localhost', port=3306,
        #                                 user='root', password='123456',
        #                                 database='mysql', charset='utf8')
        # except Exception as error:
        #     print('There is a problem connecting to MySQL！')
        #     print('Reason for failure：', error)
        #     exit()
        # self.hostname = 'localhost'
        # self.portnumber = 6379
        # self.password = '654321'
        # self.last_index = 0
        # self.r = None

    def addNeighbor(self, neighbor):
        self.neighbor.append(neighbor)

    def broadcastBC(self):  # 广播
        myChain = []
        for block in self.blockchain:
            myChain.append(self.blocktoJson(block))
        data = {
            'blocks': str(myChain),
            'length': len(myChain)
        }
        for neighbor in self.neighbor:
            response = requests.post(f'http://localhost:{neighbor}/broadcast', data=data)
            if response.status_code == 200:
                print('广播成功')
            else:
                print('广播失败')

    def blocktoJson(block):
        dir = {}
        dir["id"] = block['id']
        dir["Rwdata"] = str(block['Rwdata'])
        dir["timestamp"] = block['timestamp']
        dir["previous_hash"] = block['previous_hash']
        dir["current_hash"] = block['current_hash']
        dir["difficulty"] = block['difficulty']
        dir["proof"] = block['proof']
        # dir['transactions'] = "".join('%s' % a for a in block['transactions'])
        return dir

    def register_node(self, address):
        """
        Add a new node to the list of nodes
        :param address: Address of node. Eg. 'http://192.168.0.5:5000'
        """

        parsed_url = urlparse(address)
        if parsed_url.netloc:
            self.nodes.add(parsed_url.netloc)
        elif parsed_url.path:
            # Accepts an URL without scheme like '192.168.0.5:5000'.
            self.nodes.add(parsed_url.path)
        else:
            raise ValueError('Invalid URL')

    def valid_chain(self, chain):
        """
        Determine if a given blockchain is valid
        :param chain: A blockchain
        :return: True if valid, False if not
        """

        last_block = chain[0]
        current_id = 1

        while current_id < len(chain):
            block = chain[current_id]
            print(f'{last_block}')
            print(f'{block}')
            print("\n-----------\n")
            # Check that the hash of the block is correct
            last_block_hash = self.hash(last_block)
            if block['previous_hash'] != last_block_hash:
                return False

            # Check that the Proof of Work is correct
            if not self.valid_proof(last_block['proof'], block['proof'], block['difficulty']):
                return False

            last_block = block
            current_id += 1

        return True

    def resolve_conflicts(self):
        """
        This is our consensus algorithm, it resolves conflicts
        by replacing our chain with the longest one in the network.
        :return: True if our chain was replaced, False if not
        """

        neighbours = self.nodes
        new_chain = None

        # We're only looking for chains longer than ours
        max_length = len(self.chain)

        # Grab and verify the chains from all the nodes in our network
        for node in neighbours:
            response = requests.get(f'http://{node}/chain')

            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']

                # Check if the length is longer and the chain is valid
                if length > max_length and self.valid_chain(chain):
                    max_length = length
                    new_chain = chain

        # Replace our chain if we discovered a new, valid chain longer than ours
        if new_chain:
            self.chain = new_chain
            return True

        return False


    # 创建新区块
    def new_block(self, proof=100, previous_hash=1):
        # Creates a new Block and adds it to the chain
        """
        生成新块
        :param proof: <int> The proof given by the Proof of Work algorithm
        :param previous_hash: (Optional) <str> Hash of previous Block
        :return: <dict> New Block
         """
        idx = len(self.chain) + 1
        Rw = hashlib.sha256(" ".join('%s' %a for a in self.currentRw).encode('utf-8')).hexdigest()
        i = hashlib.sha256(str(idx).encode('utf-8')).hexdigest()
        ts = hashlib.sha256(datetime.now().strftime("%m%d%Y%H%M%S").encode('utf-8')).hexdigest()
        ph = hashlib.sha256(str(previous_hash).encode('utf-8')).hexdigest()
        p = hashlib.sha256(str(proof).encode('utf-8')).hexdigest()
        crt_hash = hashlib.sha256(str(Rw + i + ts + ph + p).encode('utf-8')).hexdigest()

        block = {

            'id': len(self.chain) + 1,   # 区块编号
            # 'AntiCounterfeitingNum':
            # 'DealerName':
            # 'Manufacturer':
            # 'ProductionTime':
            # 'ProductionArea':
            # 'LogisticsInformation':

            'Rwdata': str(self.currentRw),  # 红酒信息 data
            'timestamp': time(),  # 时间戳 创建时间
            'previous_hash': previous_hash or self.hash(self.chain[-1]),  # 前一个区块的哈希值
            'current_hash': crt_hash,
            'difficulty': 5,  # The number of 0-bits at the beginning of current_hash
            'proof': proof,  # 矿工通过工作量证明成功得到的Number Once值，证明其合法创建了一个区块（当前区块）Nonce
        }
        # if block['index'] == 1:
        #     block['current_hash'] = '0' * block['difficulty'] + block['current_hash']
        # Reset the current list of transactions
        '''
        因为已经将待处理（等待写入下一个新创建的区块中）交易信息列表（变量是：transactions）
        中的所有交易信息写入了区块并添加到区块链末尾，则此处清除此列表中的内容'
        '''
        self.currentRw = []
        # 将当前区块添加到区块链末端
        self.chain.append(block)
        return block

    # 创建新红酒
    #AntiCounterfeitingNum
    def newrw(self, DealerName, Manufacturer,ProductionTime,ProductionArea,LogisticsInformation):
        self.currentRw.append({
            'AntiCounterfeitingNum': hash(DealerName+Manufacturer+ProductionTime+ProductionArea+LogisticsInformation),
            'DealerName': DealerName,
            'Manufacturer': Manufacturer,
            'ProductionTime': ProductionTime,
            'ProductionArea': ProductionArea,
            'LogisticsInformation': LogisticsInformation,
        })

        return self.last_block['id'] + 1

    # 创建新交易
    def new_transaction(self, sender, recipient, amount):
        # Adds a new transaction to the list of transactions
        """
                生成新交易信息，此交易信息将加入到下一个待挖的区块中
                :param sender: Address of the Sender  # 发送方
                :param recipient: Address of the Recipient # 接收方
                :param amount: Amount  # 数量
                :return: The index of the Block that will hold this transaction # 需要将交易记录在下一个区块中
        """
        self.currentTransaction.append({
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
        })

        # 下一个待挖的区块中
        return self.last_block['id'] + 1


    @staticmethod
    def hash(block):
        # 根据一个区块 来生成这个区块的哈希值（散列值）
        """
               生成块的 SHA-256 hash值
               :param block: <dict> Block
               :return: <str>
               转化为json编码格式之后hash，最后以16进制的形式输出
         """

        # 我们必须确保字典是有序的，否则我们会有不一致的哈希值，sort_keys=True指明了要进行排序
        '''
        首先通过json.dumps方法将一个区块打散，并进行排序（保证每一次对于同一个区块都是同样的排序）
        这个时候区块被转换成了一个json字符串（不知道怎么描述）
        然后，通过json字符串的encode()方法进行编码处理。
        其中encode方法有两个可选形参，第一个是编码描述字符串，另一个是预定义错误信息
        默认情况下，编码描述字符串参数就是：默认编码为 'utf-8'。此处就是默认编码为'utf-8'
        '''
        #block_string = json.dumps(block, sort_keys=True).encode()
        # hexdigest(…)以16进制的形式输出
        #return hashlib.sha256(block_string).hexdigest()
        return block['current_hash'];

    @property
    def last_block(self):
        return self.chain[-1]  # 区块链的最后一个区块

    # 工作量证明
    def proof_of_work(self, lastProof,difficulty):
        """
        简单的工作量证明:
         - 查找一个 p' 使得 hash(pp') 以4个0开头
         - p 是上一个块的证明,  p' 是当前的证明
        :param last_proof: <int>
        :return: <int>
        """

        # #下面通过循环来使proof的值从0开始每次增加1来进行尝试，直到得到一个符合算法要求的proof值为止
        proof = 0
        while self.valid_proof(lastProof, proof, difficulty) is False:
            proof += 1  # 如果得到的proof值不符合要求，那么就继续寻找。
        # 返回这个符合算法要求的proof值
        return proof

    #  此函数是上一个方法函数的附属部分，用于检查哈希值是否满足挖矿条件。用于工作函数的证明中
    @staticmethod
    def valid_proof(lastProof, proof, difficulty):
        """
        验证证明: 是否hash(last_proof, proof)以4个0开头?
        :param last_proof: <int> Previous Proof
        :param proof: <int> Current Proof
        :return: <bool> True if correct, False if not.
        """
        # 根据传入的参数proof来进行尝试运算，得到一个转码为utf-8格式的字符串
        guess = f'{lastProof}{proof}{difficulty}'.encode()
        # 将此字符串（guess）进行sha256方式加密，并转换为十六进制的字符串
        guessHash = hashlib.sha256(guess).hexdigest()
        # 验证该字符前difficulty位是否为0，如果符合要求，就返回True，否则 就返回False
        zerobits = ['0'] * difficulty
        return guessHash[:difficulty] == ''.join(zerobits)

    # change difficulty if needed
    def change_difficulty(self, block):
        # only change if more than 2*count is no the chain
        if (len(self.chain) <= DIFFICULTY_COUNT * 2):
            return block['difficulty']
        # calculate average of last three by curr block's timestamp - prev timestamp
        this_round_time = (block['timestamp'] - self.chain[-DIFFICULTY_COUNT]['timestamp'])
        last_round_time = (self.chain[-DIFFICULTY_COUNT]['timestamp'] -
                           self.chain[-(DIFFICULTY_COUNT * 2)]['timestamp'])
        # if this round time > twice last round time, reduce difficulty
        if (this_round_time > last_round_time*2):
            return block['difficulty'] - 1
        # if this round tiem < half last round time, increase difficulty
        if (this_round_time < last_round_time/2):
            return block['difficulty'] + 1
        return block['difficulty']

    # Query data
    def get_data(self, id):

        with self.conn.cursor() as cursor:
            try:
                # Perform MySQL query operations
                cursor.execute('select * from tb_blcokchain '
                               'where id=%s', (id))
                result_sql = cursor.fetchall()
                print(result_sql)

                return result_sql
            except Exception as error:
                print(error)
            finally:
                self.conn.close()

    def post_data(self, block):
        with blockchain.conn.cursor() as cursor:
            try:
                # Insert the SQL statement, and result is the returned result
                res_info = cursor.execute(
                    'insert into tb_blockchain values %d, %s, %s, %s,%s, %d, %d',
                    (block['id'], block['Rwdata'], block['timestamp'], block['previous_hash'],
                     block['current_hash'],
                     block['difficulty'], block['proof']));

                # A successful insert requires a commit to synchronize in the database
                if isinstance(res_info, int):
                    print('数据更新成功')
                    blockchain.conn.commit()
            finally:
                # After the operation is complete, you need to close the connection
                blockchain.conn.close()

    def connect_to_db(self):
        """ Establishes connection with redis """
        r = redis.Redis(host=self.hostname,
                        port=self.portnumber,
                        password=self.password)
        try:
            r.ping()
        except redis.ConnectionError:
            sys.exit('ConnectionError: is the redis-server running?')
        self.r = r

    def ingest_to_db_stream(self, data):
        """ Args:
            data (string)
        """
        self.r.rpush('stream', json.dumps(data))

    def pull_and_store_stream(self, b):
        # Check if the blockchain is full
        for data in b.stream_from(full_blocks=True):
            self.ingest_to_db_stream(data)


# 实例化我们的节点；加载 Flask 框架
app = Flask(__name__)

# 为我们的节点创建一个随机名称
node_identifier = str(uuid4()).replace('-', '')

# 实例化 Blockchain 类
blockchain = Blockchain()

def strtoRWJson(rw:str)->dict:
    temp  = rw.split('&')
    return {
        # 'AntiCounterfeitingNum': temp[0].split('=')[1],
        'DealerName':temp[0].split('=')[1],
        'Manufacturer':temp[1].split('=')[1],
        'ProductionTime': temp[2].split('=')[1],
        'ProductionArea':temp[3].split('=')[1],
        'LogisticsInformation': temp[4].split('=')[1],
    }

@app.route("/rwinformation",methods=['POST'])
def rwinformation():

    # print("11111")
    values = request.get_json()
    # print("values1:" + values)
    valueslist = strtoRWJson(values)
    # print("values2:" + values)

    required = ['DealerName', 'Manufacturer', 'ProductionTime','ProductionArea', 'LogisticsInformation' ]
    if not all(k in valueslist for k in required):
        return 'Missing values', 400  # 400 请求错误


    id = blockchain.newrw(valueslist['DealerName'], valueslist['Manufacturer'],valueslist['ProductionTime'],valueslist['ProductionArea'],valueslist['LogisticsInformation']  )
    response = {'message': f'Rwdata will be added to Block {id}'}
    return jsonify(response), 201


# # 创建 /transactions/new 端点，这是一个 POST 请求，我们将用它来发送数据
# @app.route('/transactions/new', methods=['POST'])
# def new_transaction():
#
#     values = request.get_json()
#
#     required = ['sender', 'recipient', 'amount']
#     if not all(k in values for k in required):
#         return 'Missing values', 400  # 400 请求错误
#
#     index = blockchain.new_transaction(values['sender'], values['recipient'], values['amount'])
#     response = {'message': f'Transaction will be added to Block {index}'}
#     return jsonify(response), 201



# 创建 /mine 端点，这是一个GET请求
@app.route('/mine', methods=['GET'])
def mine():
    # 我们运行工作证明算法来获得下一个证明
    last_block = blockchain.last_block  # 取出区块链现在的最后一个区块
    last_proof = last_block['proof']  # 取出这最后 一个区块的哈希值（散列值）
    last_difficulty = blockchain.change_difficulty(last_block);
    proof = blockchain.proof_of_work(last_proof, last_difficulty)  # 获得了一个可以实现优先创建（挖出）下一个区块的工作量证明的proof值。

    # 由于找到了证据，我们会收到一份奖励
    # sender为“0”，表示此节点已挖掘了一个新货币
    # blockchain.newrw(
    #     AntiCounterfeitingNum="0",
    #     DealerName="Alice",
    #     Manufacturer="Bob",
    #     ProductionTime="1900-01-01",
    #     ProductionArea="New York",
    #     LogisticsInformation="NYC->HK"
    # )

    # 将新块添加到链中打造新的区块
    previous_hash = blockchain.hash(last_block)  # 取出当前区块链中最长链的最后一个区块的Hash值，用作要新加入区块的前导HASH（用于连接）
    block = blockchain.new_block(proof, previous_hash)  # 将新区块添加到区块链最后
    block['difficulty'] = last_difficulty
    #current_hash = blockchain.hash(block)
    len0  = len(block['current_hash']) - len(block['current_hash'].lstrip('0'))
    block['current_hash'] = '0' * (block['difficulty']-len0) + block['current_hash']

    response = {
        'message': "New Block Forged",
        'id': block['id'],
        'Rwdata': str(block['Rwdata']),
        'timestamp':block['timestamp'],
        'proof': block['proof'],
        'previous_hash': block['previous_hash'],
        'current_hash': block['current_hash'],
        'difficulty': block['difficulty']
    }

    return jsonify(response), 200


@app.route("/addneighbor",methods=['POST'])
def addNeighbor():
    node=request.values.get("node")
    if node==None:
        return "can not add",400
    if node not in blockchain.neighbor:
        blockchain.addNeighbor(node)
        response={
        "message":"successful",
    }
    else:
        response={
            "message":"already added"
        }
    for neighbor in blockchain.neighbor:
        print(neighbor)
    return jsonify(response),200


def blocktoJson(block):
    dir = {}
    dir['id'] = block['id']
    dir['timestamp'] = block['timestamp']
    dir['previous_hash'] = block['previous_hash']
    dir['current_hash'] = block['current_hash']
    dir['difficulty'] =block['difficulty']
    dir['proof'] = block['proof']
    dir['Rwdata'] = "".join('%s' %a for a in block['Rwdata'])
    return dir


def strtoQJson(qu:str)->dict:
    temp  = qu.split('&')
    return {
        'Info': temp[0].split('=')[1],
        'Method':temp[1].split('=')[1],
    }

@app.route("/query", methods=['POST'])
def query():

    print(222)
    values = request.get_json()
    valueslist = strtoQJson(values)

    print("values:"+values)
    print("valueslist:" + str(valueslist))

    blocks = blockchain.chain
    chain = []
    for block in blocks:
        if valueslist['Info'] in block['Rwdata']:
            chain.append(block)

    # print("chain:" + str(chain))

    response = {
        'blocks': chain,
        'length': len(chain),
        'message': 'successful'
    }
    return jsonify(response), 200


@app.route("/getblocks",methods=['GET'])
def getBlocks():
    blocks=blockchain.chain
    chain=[]
    for block in blocks:
        chain.append(blocktoJson(block))
    response={
        'blocks':chain,
        'length':len(blocks),
        'message':'successful'
    }
    return jsonify(response),200


def handleBC(blocks: str):
    blockchain = []
    for temp in blocks.split('}')[:-1]:
        r1 = re.search("\"[\\w]+\":[\"]*.+[\"]*", temp)
        result = str(r1.group())
        print(result)
        result = '{' + result + '}'
        result_dir = eval(result)

        # TODO
        newBlock = {
            'id':result_dir['id'],
            'Rwdata' : handleTX(result_dir['Rwdata']),
            'timestamp' : result_dir['timestamp'],
            'previous_hash' : result_dir['previous_hash'],
            'current_hash': result_dir['current_hash'],
            'difficulty':result_dir['difficulty'],
            'proof' : result_dir['proof']
        }

        blockchain.append(newBlock)
    return blockchain

    #TODO 处理tx字符串
def handleTX(tx:str)->list:
    txlist=[]
    for temp in tx.split("}")[:-1]:
        r1=re.search("\'[a-z]+\': .+",temp)
        result=str(r1.group())
        result='{'+result+'}'
        result_dir=eval(result)
        txlist.append(str(result_dir))
    return txlist


@app.route("/broadcast", methods=['POST'])
def broadcast():
    length = request.form.get("length")
    blocks = request.form.get("blocks")

    if blocks == None:
        return "no blocks", 400

    if int(length) > len(blockchain.chain):
        blockchain.chain = handleBC(blocks)

        # print(Node.blockchain)
    response = {
        'message': 'get the broadcast'
    }
    return jsonify(response), 200

@app.route('/nodes/register', methods=['POST'])
def register_nodes():
    values = request.get_json()

    nodes = values.get('nodes')
    if nodes is None:
        return "Error: Please supply a valid list of nodes", 400

    for node in nodes:
        blockchain.register_node(node)

    response = {
        'message': 'New nodes have been added',
        'total_nodes': list(blockchain.nodes),
    }
    return jsonify(response), 201


@app.route('/nodes/resolve', methods=['GET'])
def consensus():
    replaced = blockchain.resolve_conflicts()

    if replaced:
        response = {
            'message': 'Our chain was replaced',
            'new_chain': blockchain.chain
        }
    else:
        response = {
            'message': 'Our chain is authoritative',
            'chain': blockchain.chain
        }

    return jsonify(response), 200


# 创建 /chain 端点，它是用来返回整个 Blockchain类
@app.route('/chain', methods=['GET'])
# 将返回本节点存储的区块链条的完整信息和长度信息。
def full_chain():
    response = {
        'chain': blockchain.chain,
        'length': len(blockchain.chain),
    }
    return jsonify(response), 200






# server setting
# def serve():
#     port = '5002'
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#    # blockchain_pb2_grpc.add_BlockChainServicer_to_server(BlockchainServer(), server)
#     server.add_insecure_port('127.0.0.1:' + port)
#     server.start()
#     print("blockchain started, listening on " + port)
#     server.wait_for_termination()



# 设置服务器运行端口为 5000
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5002)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.run()
    # logging.basicConfig()
    # serve()
