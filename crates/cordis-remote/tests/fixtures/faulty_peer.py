"""Inject current protobuf frame faults; never run or report user recovery."""
from binary_peer import receive, encode
import struct
import sys

while True:
    request = receive()
    value = {}
    if request['command'] == 'hello':
        p = request['payload']
        value = {k:p[k] for k in ('contract','interface','dependency_interfaces')}
        value['http'] = None
    response = {k:request[k] for k in ('session','activation','operation')}
    response['outcome'] = {'kind':'success','value':value}
    data = encode(response)
    if request['command'] == 'call':
        fault = sys.argv[1]
        if fault == 'session': response['session'] = '0' * 32
        elif fault == 'activation': response['activation'] = dict(response['activation'], epoch='999')
        elif fault == 'operation': response['operation'] = '999'
    if request['command'] == 'call':
        from binary_peer import transport_pb2
        frame=transport_pb2.Frame.FromString(data)
        frame.session=response['session']
        frame.operation=response['operation']
        frame.activation.epoch=response['activation']['epoch']
        data=frame.SerializeToString()
        if fault == 'unknown_field': data += b'\xf8\x07\x01'
        elif fault == 'truncated': data = b'\x80'
    sys.stdout.buffer.write(struct.pack('>I',len(data))+data)
    sys.stdout.buffer.flush()
