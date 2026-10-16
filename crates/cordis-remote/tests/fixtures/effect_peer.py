"""Fault injection for effect registration, authority, and cleanup acknowledgements."""
from binary_peer import receive, send, respond
import os
import struct
import sys

mode, trace = sys.argv[1:]


def record(event):
    with open(trace, 'a', encoding='utf-8', newline='\n') as output:
        output.write(event + '\n')






next_operation = 0
pending = {}
grant = None
saved_context = None


def reverse(request, command, payload, action):
    global next_operation
    next_operation += 1
    callback = {key: request[key] for key in ('session', 'activation')}
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
            if mode == 'activation_fail':
                respond(
                    original,
                    error={'code': 'activation_failed', 'message': 'after registration'},
                )
            else:
                respond(original, {})
        elif action == 'cleanup_reject':
            assert (
                outcome['kind'] == 'refused' and outcome['error']['code'] == 'closed'
            ), outcome
            record('registration_rejected')
            respond(original, {})
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
                'capabilities': {'effects': True, 'reverse_calls': True},
            },
        )
    elif command == 'activate':
        if mode == 'activation_fail':
            reverse(
                request,
                'effect_register',
                {
                    'context': payload['context'],
                    'effect': 'e1',
                    'label': 'remote test effect',
                },
                'register',
            )
        else:
            respond(request, {})
    elif command == 'deactivate':
        record('deactivated')
        respond(request, {})
        break
    elif command == 'effect_release':
        record('released')
        if mode in ('cleanup_crash', 'dispose_crash'):
            os._exit(71)
        if mode == 'cleanup_hang':
            continue
        if mode == 'normal':
            reverse(
                request,
                'effect_register',
                {
                    'context': payload['context'],
                    'effect': 'e2',
                    'label': 'forbidden in cleanup',
                },
                'cleanup_reject',
            )
        else:
            respond(request, {})
    elif command == 'call':
        method = payload['method']
        if method == 'cache':
            saved_context = payload['context']
            respond(request, {})
        elif method in ('subscribe', 'stale', 'duplicate'):
            token = saved_context if method == 'stale' else payload['context']
            reverse(
                request,
                'effect_register',
                {'context': token, 'effect': 'e1', 'label': 'remote test effect'},
                'register' if method == 'subscribe' else 'outcome',
            )
            if mode == 'registration_crash':
                os._exit(72)
        elif method in ('dispose', 'forged', 'unknown'):
            reverse(
                request,
                'effect_dispose',
                {
                    'effect': 'e2' if method == 'unknown' else 'e1',
                    'grant': '0' * 32 if method == 'forged' else grant,
                },
                'outcome',
            )
        elif method == 'crash':
            os._exit(73)
