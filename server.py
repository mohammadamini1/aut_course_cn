#!/usr/bin/python3

import socket
import threading
import pickle
import time

HOST = '127.0.0.1'   # Standard loopback interface address (localhost)
PORT = 1374          # Port to listen on (non-privileged ports are > 1023)
subscribers = []

class Subscriber():
    def __init__(self, conn, addr, topics):
        self.conn = conn
        self.ip = addr[0]
        self.port = addr[1]
        self.topics = topics

    def is_subscribed_to_topic(self, topic):
        return topic in self.topics

    def send(self, command, args):
        data = {
            'command': command,
        }
        data.update(args)
        self.conn.sendall(pickle.dumps(data))



def recv(s, timeout=None):
    s.settimeout(timeout)
    r = s.recv(1024)
    if not r: return r
    try:
        r = pickle.loads(r)
        r['command']
    except:
        # print(r)
        raise Exception('bad response format from client')
    return r



def handler(conn, addr):
    with conn:
        print('\nConnected by', addr)
        global subscribers
        while True:
            data = recv(conn)
            if not data:
                for c in range(subscribers.__len__()):
                    if subscribers[c].port == addr[1] and subscribers[c].ip == addr[0]:
                        cc = subscribers.pop(c)
                        print('connection {}:{} disconnected'.format(cc.ip, cc.port))
                        break
                break

            try:
                #! publish
                if data['command'] == 'publish':
                    try:
                        topic = data['topic']
                        message = data['message']
                    except:
                        raise Exception('client data format not valid. received data: {}'.format(data))

                    Subscriber(conn, addr, []).send('puback', {})
                    print('get new publish topic: {}, message: {}'.format(topic, message))

                    subscribers_count = 0
                    valid_subscribers = []
                    for subscriber in subscribers:
                        if subscriber.is_subscribed_to_topic(topic):
                            try:
                                subscriber.send('message', {
                                    'topic': topic,
                                    'message': message,
                                })
                            except Exception as e:
                                # print(str(e))
                                print("cant sent to subscriber {}: {}   subscriber removed.".format(subscriber.ip, subscriber.port))
                                continue
                            valid_subscribers.append(subscriber)
                            subscribers_count += 1
                            print('send to subscriber {}:{}'.format(subscriber.ip, subscriber.port))
                    subscribers = valid_subscribers
                    print('finish publishing. subscribers count: {}'.format(subscribers_count))
                    break

                #! subscribe
                elif data['command'] == 'subscribe':
                    topics = data['topics']

                    subscriber = Subscriber(conn, addr, topics)
                    subscribers.append(subscriber)
                    subscriber.send('suback', {})
                    print('add new subscriber on topics: {}'.format(topics))

                    time.sleep(1)
                    ping = threading.Thread(target=check_clients_connection, args=(subscriber, ))
                    ping.start()
                    ping.join()
                    break

                #! ping
                elif data['command'] == 'ping':
                    Subscriber(conn, addr, []).send('pong', {})
                    print('answered pong to ping')
                    break

                else:
                    raise Exception('bad data')

            except Exception as e:
                print(str(e))
                break
        conn.close()
        print('Disconnected by', addr)



def check_clients_connection(subscriber):
    fails_count = 0
    disconnect_after_count = 3
    global subscribers
    while True:
        if not subscriber in subscribers:
            break
        time.sleep(10)
        try:
            subscriber.send('ping', {})
            data = recv(subscriber.conn, 10)
            # print('data: ', data)
            if not data:
                raise BrokenPipeError()
            if data['command'] != 'pong':
                raise Exception('command is not pong')
            # print('get pong {}:{}'.format(subscriber.ip, subscriber.port))
        except BrokenPipeError as e:
            subscribers.remove(subscriber)
            print('client {}:{} broken pipe. disconnecting ...'.format(subscriber.ip, subscriber.port))
            break
        except Exception as e:
            fails_count += 1
            print('client {}:{} failed ping. fails count: {} Exception: {}'.format(subscriber.ip, subscriber.port, fails_count, e))

        if fails_count >= disconnect_after_count:
            subscriber.conn.close()
            subscribers.remove(subscriber)
            print('disconnecting {}:{} after 3 failed ping ...'.format(subscriber.ip, subscriber.port))
            break




with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("server start listening on port ", PORT)

    while True:
        conn, addr = s.accept()
        threading.Thread(target=handler, args=(conn, addr)).start()








