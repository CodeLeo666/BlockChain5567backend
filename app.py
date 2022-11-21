import hashlib
import json
from datetime import datetime
from random import random
from time import time
from uuid import uuid4

import ecdsa as ecdsa
from flask import Flask, jsonify, request
from urllib.parse import urlparse
from werkzeug.middleware.proxy_fix import ProxyFix
import requests
import re
import redis
import sys

portval = 5002
DIFFICULTY_COUNT = 3

class Blockchain(object):
    def __init__(self):
        self.chain = []
        self.currentTransaction = []
        self.currentRw = []
        self.nodes = set()
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

    def broadcastBC(self):
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
                print('Broadcasting succeeded')
            else:
                print('Broadcast failed')

    def blocktoJson(block):
        dir = {}
        dir["id"] = block['id']
        dir["Rwdata"] = str(block['Rwdata'])
        dir["timestamp"] = block['timestamp']
        dir["previous_hash"] = block['previous_hash']
        dir["current_hash"] = block['current_hash']
        dir["difficulty"] = block['difficulty']
        dir["proof"] = block['proof']
        return dir

    def register_node(self, address):

        parsed_url = urlparse(address)
        if parsed_url.netloc:
            self.nodes.add(parsed_url.netloc)
        elif parsed_url.path:
            self.nodes.add(parsed_url.path)
        else:
            raise ValueError('Invalid URL')

    def valid_chain(self, chain):

        last_block = chain[0]
        current_id = 1

        while current_id < len(chain):
            block = chain[current_id]
            print(f'{last_block}')
            print(f'{block}')
            print("\n-----------\n")
            last_block_hash = self.hash(last_block)
            if block['previous_hash'] != last_block_hash:
                return False

            if not self.valid_proof(last_block['proof'], block['proof'], block['difficulty']):
                return False

            last_block = block
            current_id += 1

        return True

    def resolve_conflicts(self):

        neighbours = self.nodes
        new_chain = None

        max_length = len(self.chain)

        for node in neighbours:
            response = requests.get(f'http://{node}/chain')

            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']

                if length > max_length and self.valid_chain(chain):
                    max_length = length
                    new_chain = chain

        if new_chain:
            self.chain = new_chain
            return True

        return False


    def new_block(self, proof=100, previous_hash=1):

        idx = len(self.chain) + 1
        Rw = hashlib.sha256(" ".join('%s' %a for a in self.currentRw).encode('utf-8')).hexdigest()
        i = hashlib.sha256(str(idx).encode('utf-8')).hexdigest()
        ts = hashlib.sha256(datetime.now().strftime("%m%d%Y%H%M%S").encode('utf-8')).hexdigest()
        ph = hashlib.sha256(str(previous_hash).encode('utf-8')).hexdigest()
        p = hashlib.sha256(str(proof).encode('utf-8')).hexdigest()
        crt_hash = hashlib.sha256(str(Rw + i + ts + ph + p).encode('utf-8')).hexdigest()

        block = {

            'id': len(self.chain) + 1,
            'Rwdata': str(self.currentRw),
            'timestamp': time(),
            'previous_hash': previous_hash or self.hash(self.chain[-1]),
            'current_hash': crt_hash,
            'difficulty': 5,
            'proof': proof,
        }

        self.currentRw = []
        self.chain.append(block)
        return block

    def newrw(self, DealerName, Manufacturer,ProductionTime,ProductionArea,LogisticsInformation):

        gen = ecdsa.NIST256p.generator
        order = gen.order()
        # Generate private key d_ A
        d_A = random.randrange(1, order - 1)
        # Generate public and private key objects
        public_key = ecdsa.ecdsa.Public_key(gen, gen * d_A)
        private_key = ecdsa.ecdsa.Private_key(public_key, d_A)
        message = DealerName + Manufacturer+ProductionTime+ProductionArea+LogisticsInformation
        m = int(hashlib.sha1(message.encode("utf8")).hexdigest(), 16)
        # Temporary Key
        k = random.randrange(1, order - 1)

        self.currentRw.append({
            'AntiCounterfeitingNum': hash(DealerName+Manufacturer+ProductionTime+ProductionArea+LogisticsInformation),
            'DealerName': DealerName,
            'Manufacturer': Manufacturer,
            'ProductionTime': ProductionTime,
            'ProductionArea': ProductionArea,
            'LogisticsInformation': LogisticsInformation,
        })

        return self.last_block['id'] + 1

    def new_transaction(self, sender, recipient, amount):

        self.currentTransaction.append({
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
        })

        return self.last_block['id'] + 1


    @staticmethod
    def hash(block):
        return block['current_hash'];

    @property
    def last_block(self):
        return self.chain[-1]

    def proof_of_work(self, lastProof,difficulty):

        proof = 0
        while self.valid_proof(lastProof, proof, difficulty) is False:
            proof += 1
        return proof

    @staticmethod
    def valid_proof(lastProof, proof, difficulty):
        guess = f'{lastProof}{proof}{difficulty}'.encode()
        guessHash = hashlib.sha256(guess).hexdigest()
        zerobits = ['0'] * difficulty
        return guessHash[:difficulty] == ''.join(zerobits)

    def change_difficulty(self, block):
        if (len(self.chain) <= DIFFICULTY_COUNT * 2):
            return block['difficulty']
        this_round_time = (block['timestamp'] - self.chain[-DIFFICULTY_COUNT]['timestamp'])
        last_round_time = (self.chain[-DIFFICULTY_COUNT]['timestamp'] -
                           self.chain[-(DIFFICULTY_COUNT * 2)]['timestamp'])
        if (this_round_time > last_round_time*2):
            return block['difficulty'] - 1
        if (this_round_time < last_round_time/2):
            return block['difficulty'] + 1
        return block['difficulty']

    def get_data(self, id):

        with self.conn.cursor() as cursor:
            try:
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
                res_info = cursor.execute(
                    'insert into tb_blockchain values %d, %s, %s, %s,%s, %d, %d',
                    (block['id'], block['Rwdata'], block['timestamp'], block['previous_hash'],
                     block['current_hash'],
                     block['difficulty'], block['proof']));

                if isinstance(res_info, int):
                    # print('数据更新成功')
                    blockchain.conn.commit()
            finally:
                blockchain.conn.close()

    def connect_to_db(self):
        r = redis.Redis(host=self.hostname,
                        port=self.portnumber,
                        password=self.password)
        try:
            r.ping()
        except redis.ConnectionError:
            sys.exit('ConnectionError: is the redis-server running?')
        self.r = r

    def ingest_to_db_stream(self, data):
        self.r.rpush('stream', json.dumps(data))

    def pull_and_store_stream(self, b):
        for data in b.stream_from(full_blocks=True):
            self.ingest_to_db_stream(data)

app = Flask(__name__)

node_identifier = str(uuid4()).replace('-', '')

blockchain = Blockchain()

def strtoRWJson(rw:str)->dict:
    temp  = rw.split('&')
    return {
        'DealerName':temp[0].split('=')[1],
        'Manufacturer':temp[1].split('=')[1],
        'ProductionTime': temp[2].split('=')[1],
        'ProductionArea':temp[3].split('=')[1],
        'LogisticsInformation': temp[4].split('=')[1],
    }

@app.route("/rwinformation",methods=['POST'])
def rwinformation():

    values = request.get_json()
    valueslist = strtoRWJson(values)


    required = ['DealerName', 'Manufacturer', 'ProductionTime','ProductionArea', 'LogisticsInformation' ]
    if not all(k in valueslist for k in required):
        return 'Missing values', 400

    id = blockchain.newrw(valueslist['DealerName'], valueslist['Manufacturer'],valueslist['ProductionTime'],valueslist['ProductionArea'],valueslist['LogisticsInformation']  )
    response = {'message': f'Rwdata will be added to Block {id}'}
    return jsonify(response), 201

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


@app.route('/mine', methods=['GET'])
def mine():

    while True:

        last_block = blockchain.last_block
        last_proof = last_block['proof']
        last_difficulty = blockchain.change_difficulty(last_block);
        proof = blockchain.proof_of_work(last_proof, last_difficulty)

        # blockchain.newrw(
        #     AntiCounterfeitingNum="0",
        #     DealerName="Alice",
        #     Manufacturer="Bob",
        #     ProductionTime="1900-01-01",
        #     ProductionArea="New York",
        #     LogisticsInformation="NYC->HK"
        # )

        previous_hash = blockchain.hash(last_block)
        block = blockchain.new_block(proof, previous_hash)
        block['difficulty'] = last_difficulty
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

    values = request.get_json()
    valueslist = strtoQJson(values)

    # print("query-valueslist:" + str(valueslist))

    blocks = blockchain.chain
    chain = []

    for block in blocks:
        if valueslist['Info'] in block['Rwdata']:
            chain.append(block)

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


@app.route('/chain', methods=['GET'])
def full_chain():
    response = {
        'chain': blockchain.chain,
        'length': len(blockchain.chain),
    }
    return jsonify(response), 200



if __name__ == '__main__':

    portval = input('Please input the number of node:')
    app.run(host='127.0.0.1', port=int(portval))
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.run()

