import json


def output_parse(out_str: str = ''):
    new_out_str: str = ''
    info_dic = []
    for line in out_str.splitlines():
        if line.startswith('#LOG#'):
            if line.startswith('#LOG#NODEINFO#'):
                info = line.split('#LOG#NODEINFO#')[1]
                info_dic.append(json.loads(info))
        else:
            new_out_str += line + "\n"
    out_dic = {"node_info": info_dic, "output": new_out_str}
    return out_dic


def camel_case_split(s):
    words = [[s[0]]]

    for c in s[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)

    return ' '.join([''.join(word) for word in words])
