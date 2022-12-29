import json
import time
import traceback
import ast
from json import JSONDecodeError
from typing import Dict

from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
# Create your views here.
from django.views.decorators.csrf import csrf_exempt

from .models.workspace import Workspace, GeneralNode, InitialNode, NonInitialNode


def get_workspace():
    ws = Workspace.get_workspace()
    if not ws.engine_type:
        ws.engine_type = 'pandas'
        ws.new_engine()
    return ws


def my_processor(sleep_interval):
    lines = [
        'Little brown lady',
        'Jumped into the blue water',
        'And smiled'
    ]
    start_time = time.time()
    while True:
        for line in lines:
            elapsed_time = int(time.time() - start_time)
            print(f"[{elapsed_time:>10} s] {line}\n")
            yield f"[{elapsed_time:>10} s] {line}\n"
            time.sleep(sleep_interval)
        yield "=========== Here we go again ===========\n"


def streamed(request):
    print("Here")
    sleep_interval = int(request.GET.get('sleep', 1))
    response = StreamingHttpResponse(my_processor(sleep_interval), content_type='text')
    return response


@csrf_exempt
def reset_workspace(request):
    if request.method == 'POST':
        ws = get_workspace()
        ws.reset_workspace()
        return HttpResponse("Success")
    return HttpResponse("BAD")


@csrf_exempt
def create_node(request, node_name):
    if request.method != 'POST':
        return HttpResponse("BAD")

    ws = get_workspace()
    if request.body:
        data = json.loads(request.body)
    else:
        data = {}
    if node_name not in ws.available_nodes:
        return HttpResponse("BAD: node_name not available")

    data = {key: data[key] for key in data.keys() if key in ws.available_nodes[node_name].__slots__}
    node_id = ws.create_node(node_name, **data)
    res = {"node_id": node_id}
    return HttpResponse(json.dumps(res))


@csrf_exempt
def node_name_info(request, node_name):
    ws = get_workspace()
    if node_name not in ws.get_available_nodes():
        return JsonResponse({"status": "fail", "message": "Node name is not valid"})
    temp: GeneralNode = ws.get_available_nodes()[node_name]()
    ports: Dict = {}
    if isinstance(temp, InitialNode):
        ports = {
            "in_ports": 0,
            "out_ports": len(list(temp.get_out_ports().keys()))}
    elif isinstance(temp, NonInitialNode):
        ports = {
            "in_ports": len(list(temp.get_in_ports().keys())),
            "out_ports": len(list(temp.get_out_ports().keys()))
        }
    info = {
        **ports,
        "params": [x for x in ws.get_available_nodes()[node_name].__slots__ if x != '_in_port' and x != '_out_port']
    }
    if type(temp) in ws.crm_nodes.values():
        info["params"] = []
    return JsonResponse({"status": 0, "data": info})


@csrf_exempt
def default_connect_node(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        origin_node_id = data["origin_node_id"]
        dest_node_id = data["dest_node_id"]
        origin_port = 0
        dest_port = 0
        if 'origin_port' in data:
            origin_port = data["origin_port"]
        if 'dest_port' in data:
            dest_port = data["dest_port"]
        ws = get_workspace()
        ws.connect_nodes(origin_node_id, dest_node_id, origin_port, dest_port)
        return HttpResponse("Success")


@csrf_exempt
def default_disconnect_node(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        origin_node_id = data["origin_node_id"]
        dest_node_id = data["dest_node_id"]
        origin_port = 0
        dest_port = 0
        if 'origin_port' in data:
            origin_port = data["origin_port"]
        if 'dest_port' in data:
            dest_port = data["dest_port"]
        ws = get_workspace()
        ws.disconnect_nodes(origin_node_id, dest_node_id, origin_port, dest_port)
        return HttpResponse("Success")


@csrf_exempt
def edit_node(request, node_id):
    if request.method == 'POST':
        data = json.loads(request.body)
        ws = get_workspace()
        for key in data.keys():
            try:
                print(ast.literal_eval(data[key]))
                data[key] = ast.literal_eval(data[key])
            except Exception:
                pass
        node_id = int(node_id)
        data = {key: data[key] for key in data.keys() if key in ws.get_nodes()[node_id].__slots__}
        print(data)
        ws.get_nodes()[node_id].set_attribute(**data)
        return JsonResponse({"status": 0})


@csrf_exempt
def view_node(request, node_id):
    if request.method == 'GET':
        ws = get_workspace()
        node_id = int(node_id)
        data = {x: str(getattr(ws.get_nodes()[node_id], x)) for x in ws.get_nodes()[node_id].__slots__}
        return JsonResponse({"status": 0, "data": data})


@csrf_exempt
def settings(request):
    if request.method == 'GET':
        ws = get_workspace()
        print(ws)
        info = ws.get_workspace_info()
        dic: Dict = {"engine_type": info["engine_type"],
                     "save_ws_file": ws.save_ws_file,
                     "run_env": info["engine"]["run_env"],
                     "temp_dir": info["engine"]["temp_dir"]
                     }
        return JsonResponse({"status": 0, "data": dic})
    elif request.method == 'POST':
        data = json.loads(request.body)
        ws = get_workspace()
        # try:
        ws.set_attributes(**data["settings"])
        return JsonResponse({"status": 0})
        # except Exception:
        #     return JsonResponse({"status": -1})
    return JsonResponse({"status": -2})




@csrf_exempt
def remove(request, node_id):
    if request.method == 'DELETE':
        ws = get_workspace()
        node_id = int(node_id)
        res = ws.remove_node(node_id)
        if res:
            return JsonResponse({"status": 0})
        else:
            return JsonResponse({"status": -1})
    return JsonResponse({"status": -2})


@csrf_exempt
def set_nodes_pos(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        ws = get_workspace()
        for node_id, pos in data["nodes_pos"].items():
            pos_x, pos_y = pos
            ws.set_node_ui_pos(int(node_id), float(pos_x), float(pos_y))
        return JsonResponse({"status": 0})
    return JsonResponse({"status": -2})


@csrf_exempt
def get_schema(request, node_id):
    if request.method == 'GET':
        ws = get_workspace()
        node_id = int(node_id)
        ws.compile(node_id)
        schema = ws.get_schema()
        return HttpResponse(str(schema))


@csrf_exempt
def show(request, node_id):
    if request.method == 'GET':
        try:
            ws = get_workspace()
            node_id = int(node_id)
            ws.compile(node_id)
            rows = ws.show()
            return JsonResponse({"status": 0, "output": rows})
        except Exception as e:
        #     return JsonResponse({"status": -1, "output": "", "error": traceback.format_exc()})
            return JsonResponse({"status": -1, "output": "", "error": str(e)})


@csrf_exempt
def save_ws(request):
    if request.method == 'POST':
        try:
            ws = get_workspace()
            ws.save_workspace()
            return JsonResponse({"status": 0})
        except Exception as e:
            return JsonResponse({"status": -1, "error": str(e)})


@csrf_exempt
def load_ws(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            Workspace.load_workspace(data["path"])
            ws = get_workspace()
            dic: Dict = ws.get_workspace_info()
            nodes: Dict = {}
            print(dic)
            for node_id, node_obj in dic["nodes"].items():
                if not node_obj:
                    continue
                temp: GeneralNode = ws.get_nodes()[node_id]
                ports: Dict = {}
                if isinstance(temp, InitialNode):
                    ports = {
                        "in_ports": 0,
                        "out_ports": len(list(temp.get_out_ports().keys()))}
                elif isinstance(temp, NonInitialNode):
                    ports = {
                        "in_ports": len(list(temp.get_in_ports().keys())),
                        "out_ports": len(list(temp.get_out_ports().keys()))
                    }
                node_info: Dict = {
                    **ports,
                    "params": [x for x in temp.__slots__ if x != '_in_port' and x != '_out_port']
                }
                node_type: str = type(temp).__name__
                node_type = node_type[:-4]
                nodes[node_id] = {"info": node_info, "nodeType": node_type, "title": node_type}
            dic["nodes"] = nodes

            print(dic)
            return JsonResponse({"status": 0, "data": dic})
        except Exception as e:
            print(e)
            return JsonResponse({"status": -1, "error": str(e)})


@csrf_exempt
def get_curr_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_nodes()))


@csrf_exempt
def get_available_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return JsonResponse(ws.get_available_nodes_categorized())


@csrf_exempt
def get_connected_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_connected_nodes()))
