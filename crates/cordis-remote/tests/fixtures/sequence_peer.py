"""Inject uncertainty between running a step and acknowledging its inverse."""

from binary_peer import receive, send, respond
import os
import struct
import sys

mode, trace = sys.argv[1:]






def record(event):
    with open(trace, 'a', encoding='utf-8', newline='\n') as output:
        output.write(event + '\n')


pending = {}
next_operation = 0
grant = None
old_context = None


def reverse(request, command, payload, action):
    global next_operation
    next_operation += 1
    callback = {k: request[k] for k in ('session', 'activation')}
    callback.update(operation=str(next_operation), command=command, payload=payload)
    pending[callback['operation']] = (request, action)
    send(callback)


while True:
    request = receive()
    if 'outcome' in request:
        original, action = pending.pop(request['operation'])
        outcome = request['outcome']
        if action == 'register':
            assert outcome['kind'] == 'success', outcome
            grant = outcome['value']['grant']
            respond(original, {})
        elif action == 'stale':
            assert outcome['kind'] == 'refused', outcome
            assert outcome['error']['code'] == 'closed', outcome
            record('stale_rejected')
            respond(original, {'done': True, 'inverse': 'second', 'error': None})
        else:
            respond(original, outcome)
        continue
    command = request['command']
    payload = request['payload'] or {}
    if command == 'hello':
        respond(
            request,
            {
                'contract': payload['contract'], 'interface': payload['interface'],
            'dependency_interfaces': payload['dependency_interfaces'],
                
                'http': None,
                'capabilities': {'effect_sequences': True},
            },
        )
    elif command == 'activate':
        respond(request, {})
    elif command == 'call':
        method = payload['method']
        if method in ('start', 'duplicate'):
            reverse(
                request,
                'sequence_register',
                {
                    'context': payload['context'],
                    'sequence': 's1',
                    'label': 'test sequence',
                },
                'register' if method == 'start' else 'result',
            )
            if mode == 'register_crash':
                os._exit(73)
        else:
            reverse(
                request,
                {
                    'ready': 'sequence_ready',
                    'status': 'sequence_status',
                    'dispose': 'sequence_dispose',
                    'forged': 'sequence_dispose',
                }[method],
                {
                    'sequence': 's1',
                    'grant': '0' * 32 if method == 'forged' else grant,
                },
                'result',
            )
    elif command == 'sequence_next':
        step = payload['step']
        record('step:' + step)
        if step == '1':
            old_context = payload['context']
            respond(request, {'done': False, 'inverse': 'first', 'error': None})
        elif mode == 'crash':
            os._exit(71)
        elif mode == 'hang':
            continue
        elif mode == 'malformed':
            respond(request, {'done': True, 'inverse': 'second', 'error': {}})
        elif mode == 'reject':
            reply = {
                k: request[k] for k in ('session', 'activation', 'operation')
            }
            reply['outcome'] = {
                'kind': 'refused',
                'error': {'code': 'overloaded', 'message': 'not admitted'},
            }
            send(reply)
        elif mode == 'forward_with_inverse':
            respond(
                request,
                {
                    'done': True,
                    'inverse': 'second',
                    'error': {'code': 'forward_failed', 'message': 'partial result'},
                },
            )
        elif mode == 'stale':
            reverse(
                request,
                'sequence_register',
                {
                    'context': old_context,
                    'sequence': 's2',
                    'label': 'expired step',
                },
                'stale',
            )
        else:
            respond(request, {'done': True, 'inverse': 'second', 'error': None})
    elif command == 'sequence_undo':
        record('undo:' + payload['step'])
        if mode == 'undo_crash':
            os._exit(72)
        respond(request, {})
    elif command == 'sequence_release':
        record('producer_released')
        respond(request, {})
    elif command == 'deactivate':
        record('deactivated')
        respond(request, {})
        break
