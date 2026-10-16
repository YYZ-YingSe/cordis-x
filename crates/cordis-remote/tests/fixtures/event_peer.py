"""A events-only peer with explicit acknowledgement, crash and stop faults."""

from binary_peer import receive, send, respond
import os
import struct
import sys

mode, trace = sys.argv[1:]
pending = {}
next_operation = 0
grant = None
running = None
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
            if mode == 'malformed':
                respond(original, {}, raw=True)
            elif mode == 'business_error':
                respond(original, error={'code': 'closed', 'message': 'business failure'})
            elif mode == 'finite':
                respond(original, {'kind': 'continue'})
        else:
            respond(original, outcome)
        continue
    command = request['command']
    payload = request['payload'] or {}
    if command == 'hello':
        respond(request, {
            'events': payload['events'], 'contract': payload['contract'], 'interface': payload['interface'],
            'dependency_interfaces': payload['dependency_interfaces'],  'http': None,
            'capabilities': {'events': True, 'effects': True},
        })
    elif command == 'activate':
        respond(request, {})
    elif command == 'call':
        method = payload['method']
        if method in ('open', 'duplicate', 'stale', 'unbound'):
            reverse(request, 'event_register', {
                'listener': 'l2' if method == 'stale' else 'l1', 'event': 'unbound' if method == 'unbound' else 'tick',
                'context': old_context if method == 'stale' else payload['context'],
                
            }, 'register' if method == 'open' else 'result')
            if mode == 'register_crash':
                os._exit(73)
        elif method == 'forged_cleanup':
            reverse(request, 'cleanup_call', {
                'context': payload['context'], 'dependency': 'undeclared',
                'method': 'write', 'args': None,
            })
        else:
            reverse(request, 'event_dispose', {
                'listener': 'l1', 'grant': '0' * 32 if method == 'forged' else grant,
            })
    elif command == 'event_call':
        record('started')
        running = request
        old_context = payload['context']
        if mode == 'reject':
            respond(request, {'code': 'overloaded', 'message': 'not admitted'}, True)
        else:
            reverse(request, 'effect_register', {
                'effect': 'e1', 'context': payload['context'], 'label': 'task effect',
            }, 'effect')
    elif command == 'event_release':
        record('released')
        respond(request, {})
    elif command == 'effect_release':
        record('cleanup')
        respond(request, {})
    elif command == 'deactivate':
        record('deactivated')
        respond(request, {})
        break
