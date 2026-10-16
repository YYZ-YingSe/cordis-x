"""A scopes-only peer with deterministic admission, execution and reply faults."""

from binary_peer import receive, send, respond
import os
import struct
import sys

mode, trace = sys.argv[1:]
pending = {}
next_operation = 0
next_run = 0
grant = None
old_context = None






def reverse(request, command, payload, action='result'):
    global next_operation
    next_operation += 1
    callback = {k: request[k] for k in ('session', 'activation')}
    callback.update(operation=str(next_operation), command=command, payload=payload)
    pending[callback['operation']] = (request, action)
    send(callback)


def record(event):
    with open(trace, 'a', encoding='utf-8', newline='\n') as output:
        output.write(event + '\n')


while True:
    request = receive()
    if 'outcome' in request:
        original, action = pending.pop(request['operation'])
        outcome = request['outcome']
        if action == 'register':
            assert outcome['kind'] == 'success', outcome
            grant = outcome['value']['grant']
            respond(original, {})
        elif action == 'effect':
            assert outcome['kind'] == 'success', outcome
            record('registered')
            if mode == 'crash':
                os._exit(71)
            if mode == 'hang':
                continue
            if mode == 'malformed':
                respond(original, {'kind': 'success'})
            elif mode == 'business_error':
                respond(original, {'kind': 'failure', 'error': {'code': 'closed', 'message': 'business failure'}})
            else:
                respond(original, {'kind': 'success', 'value': 42})
        elif action == 'stale':
            assert outcome['kind'] == 'refused' and outcome['error']['code'] == 'closed', outcome
            record('stale_rejected')
            respond(original, {'kind': 'success', 'value': 43})
        else:
            respond(original, outcome)
        continue
    command = request['command']
    payload = request['payload'] or {}
    if command == 'hello':
        respond(request, {
            'contract': payload['contract'], 'interface': payload['interface'],
            'dependency_interfaces': payload['dependency_interfaces'],  'http': None,
            'capabilities': {'scopes': True, 'effects': True},
        })
    elif command == 'activate':
        respond(request, {})
    elif command == 'call':
        method = payload['method']
        if method in ('open', 'duplicate'):
            reverse(request, 'scope_register', {
                'scope': 'c1', 'context': payload['context'], 'label': 'test child',
            }, 'register' if method == 'open' else 'result')
            if mode == 'register_crash':
                os._exit(73)
        elif method in ('run', 'duplicate_run'):
            if method == 'run':
                next_run += 1
            reverse(request, 'scope_run', {'scope': 'c1', 'grant': grant, 'run': f'u{next_run}'})
        else:
            reverse(request, {
                'ready': 'scope_ready', 'status': 'scope_status', 'close': 'scope_dispose',
                'forged': 'scope_dispose',
            }[method], {'scope': 'c1', 'grant': '0' * 32 if method == 'forged' else grant})
    elif command == 'scope_call':
        record('run:' + payload['run'])
        if mode == 'reject':
            respond(request, {'code': 'overloaded', 'message': 'not admitted'}, True)
        elif next_run == 2:
            reverse(request, 'scope_register', {
                'scope': 'c2', 'context': old_context, 'label': 'stale callback',
            }, 'stale')
        else:
            old_context = payload['context']
            reverse(request, 'effect_register', {
                'effect': 'e1', 'context': payload['context'], 'label': 'child effect',
            }, 'effect')
    elif command == 'effect_release':
        record('cleanup')
        if mode == 'cleanup_crash':
            os._exit(72)
        respond(request, {})
    elif command == 'deactivate':
        record('deactivated')
        respond(request, {})
        break
