"""A deliberately non-cooperative peer for dependency lifetime/protocol tests."""

from binary_peer import receive, send, respond
import os
import struct
import sys






next_operation = 0
pending = {}
saved = None
blocked = None
last_callback = None
while True:
    request = receive()
    if "outcome" in request:
        original = pending.pop(request["operation"])
        if original is not None:
            respond(original, request["outcome"])
        continue
    command = request["command"]
    if command == "hello":
        respond(
            request,
            {
                "contract": request["payload"]["contract"],
                "interface": request["payload"]["interface"],
                "dependency_interfaces": request["payload"]["dependency_interfaces"],
                
                "http": None,
                "dependencies": request["payload"]["dependencies"],
                "capabilities": {"reverse_calls": True},
            },
        )
    elif command == "activate":
        respond(request, {})
    elif command == "deactivate":
        respond(request, {})
        break
    elif command == "call":
        method = request["payload"]["method"]
        if method == "cache":
            saved = request["payload"]["context"]
            respond(request, {})
            continue
        if method == "crash":
            os._exit(70)
        if method == "early_return":
            respond(blocked, {})
            for operation in pending:
                pending[operation] = None
            respond(request, {})
            continue
        if method == "duplicate":
            send(last_callback)
            continue
        next_operation += 1
        callback = {key: request[key] for key in ("session", "activation")}
        callback.update(
            operation=str(next_operation),
            command="dependency_call",
            payload={
                "context": saved
                if method == "stale"
                else request["payload"]["context"],
                "dependency": "not.declared"
                if method == "undeclared"
                else "example.logger",
                "method": "write",
                "args": "reject" if method == "business_error" else ("block" if method == "block" else "probe"),
            },
        )
        if method == "bad_session":
            callback["session"] = "0" * 32
        if method == "bad_epoch":
            callback["activation"] = dict(callback["activation"], epoch="999")
        if method == "bad_frame":
            body = b"\x80"
            sys.stdout.buffer.write(struct.pack(">I", len(body)) + body)
            sys.stdout.buffer.flush()
            continue
        if method == "block":
            blocked = request
        pending[callback["operation"]] = request
        last_callback = callback
        send(callback)
