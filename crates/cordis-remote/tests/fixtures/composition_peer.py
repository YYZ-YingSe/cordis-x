"""Bypass SDK checks to exercise native authority and lost acknowledgements."""
from binary_peer import receive, send, respond
import os
import struct
import sys

mode = sys.argv[1]
pending = {}
sequence = 0
previous = None





def reverse(original, command, payload, action):
    global sequence
    sequence += 1
    request = {k: original[k] for k in ('session', 'activation')}
    request.update(command=command, payload=payload, operation=str(sequence))
    pending[str(sequence)] = (original, action)
    send(request)


while True:
    request = receive()
    if 'outcome' in request:
        original, action = pending.pop(request['operation'])
        outcome = request['outcome']
        if action == 'activate':
            assert outcome['kind'] == 'success', outcome
            respond(original, None)
        elif action == 'loss':
            assert outcome['kind'] == 'success', outcome
            if mode == 'wait_crash':
                reverse(original, 'child_ready', outcome['value']['value'], 'return')
            os._exit(71)
        else:
            respond(original, outcome)
        continue
    command, payload = request['command'], request['payload'] or {}
    if command == 'hello':
        payload['composition']['factories'].reverse()
        respond(request, {'contract': payload['contract'], 'interface': payload['interface'],
            'dependency_interfaces': payload['dependency_interfaces'], 'composition': payload['composition'],
                           'http': None, 'capabilities': {'composition': True}})
    elif command == 'activate':
        reverse(request, 'value_provide', {'context': payload['context'], 'service': 'settings', 'value': {'answer': 42}}, 'activate')
    elif command == 'call':
        args = payload['args']
        data = dict(args['payload'])
        if 'context' not in data:
            data['context'] = previous if payload['method'] == 'stale' else payload['context']
        previous = payload['context']
        reverse(request, args['command'], data, 'loss' if mode != 'normal' and args['command'] == 'child_install' else 'return')
    elif command == 'deactivate':
        respond(request, None)
        break
