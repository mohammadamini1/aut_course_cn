#!/usr/bin/python3

import sys
import socket
import pickle


def send(s, command, args):
    data = {
        'command': command,
    }
    data.update(args)
    s.sendall(pickle.dumps(data))

def recv(s, timeout=None):
    s.settimeout(timeout)
    r = s.recv(1024)
    if not r: return r
    r = pickle.loads(r)
    try:
        r['command']
    except:
        raise Exception('bad response format from server')
    return r



if __name__ == '__main__':
    try:
        host = sys.argv[1]
        port = int(sys.argv[2])
        command = sys.argv[3].lower()
    except Exception as e:
        print(str(e))
        print('usage: {} <host> <port> <command> <command_args>'.format(sys.argv[0]))
        exit()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    #! publish
    if command == 'publish':
        try:
            topic = sys.argv[4]
            message = sys.argv[5]
            if len(sys.argv) > 6:
                raise Exception('too many argument for publish')
        except Exception as e:
            print(str(e))
            print('usage: {} <host> <port> publish <topic> <message>'.format(sys.argv[0]))
            exit()

        try:
            s.connect((host, port))
            send(s, command, {
                'topic': topic,
                'message': message,
            })

            while True:
                data = recv(s=s, timeout=10)
                if not data:
                    raise Exception('not data')

                if data['command'] == 'ping':
                    send(s, 'pong', {})
                    continue

                if data['command'] != 'puback':
                    try:
                        err = data['error']
                    except:
                        err = 'unknown'
                    raise Exception('received {} command instead of puback. error: {}'.format(data['command'], err))
                break

            s.close()
            print('your message published successfully')
        except Exception as e:
            print(str(e))
            print('your message publishing failed')
            s.close()
            exit()


    #! subscribe
    elif command == 'subscribe':
        topics = []
        for v in range(4, len(sys.argv)):
            topics.append(sys.argv[v])

        if topics == []:
            print('usage: {} <host> <port> subscribe <topic-1> <topic-2> <...>'.format(sys.argv[0]))
            exit()

        try:
            s.connect((host, port))
            send(s, command, {
                'topics': topics,
            })

            data = recv(s, timeout=10)
            if not data:
                raise Exception('not data')

            if data['command'] != 'suback':
                try:
                    err = data['error']
                except:
                    err = 'unknown'
                raise Exception('received {} command instead of suback. error: {}'.format(data['command'], err))

            print('subscribing on: {}'.format(' & '.join(topics)))
            while True:
                data = recv(s)
                if data['command'] != 'message':
                    if data['command'] == 'ping':
                        send(s, 'pong', {})
                        continue
                    raise Exception('received {} command instead of message. unknown command for subscription'.format(data['command']))

                try:
                    topic = data['topic']
                    message = data['message']
                except:
                    raise Exception('server result format not valid. received data: {}'.format(data))

                print('new: {}: {}'.format(topic, message))

        except Exception as e:
            print(str(e))
            print('subscribing failed')
            s.close()
            exit()


    #! ping
    elif command == 'ping':
        if len(sys.argv) > 4:
            raise Exception('too many argument for ping')

        try:
            s.connect((host, port))
            send(s, command, {})
            print('ping sent')
            data = recv(s, 10)
            if not data:
                raise Exception('not data')

            if data['command'] != 'pong':
                raise Exception('get {} command in ping answer instead of pong')
            else:
                print('received pong')
        except Exception as e:
            print(str(e))
            print('ping failed')
        s.close()
        exit()

    else:
        print('command {} not found'.format(command))
        exit()

    s.close()


