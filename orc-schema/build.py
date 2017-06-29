from config import *


def build_nested_schema(struct):
    schema = []
    for tup in struct:
        if type(tup) != tuple:
            print("struct type not tuple")
            return None

        size = len(tup)
        if size == 2:
            res = build_nested_schema(tup[1])
            if not res:
                return None
            schema.append("%s:%s" % (tup[0], res))
        elif size == 4:
            res = "%s:%s" % (tup[2], tup[3])
            schema.append(res)
        else:
            print("struct length not match")
            return None

    return "struct<%s>" % ",".join(schema)


def build_flat_schema_recursive(name, struct, res_dic):
    for tup in struct:
        if type(tup) != tuple:
            print("struct type not tuple")
            return None

        size = len(tup)
        if size == 2:
            temp_name = '%s_%s' % (name, tup[0])
            if not build_flat_schema_recursive(temp_name, tup[1], res_dic):
                return None
        elif size == 4:
            if tup[1] not in res_dic:
                res_dic[tup[1]] = []
            temp_name = '%s_%s' % (name, tup[2])
            res_dic[tup[1]].append("%s:%s" % (temp_name, tup[3]))
        else:
            print("struct length not match")
            return None
        
    return True
    

def build_flat_schema(struct):
    schema = []
    res_dic = {}
    for tup in struct:
        if type(tup) != tuple:
            print("struct type not tuple")
            return None

        size = len(tup)
        if size == 2:
            if not build_flat_schema_recursive(tup[0], tup[1], res_dic):
                return None
        elif size == 4:
            if tup[1] not in res_dic:
                res_dic[tup[1]] = []
            res_dic[tup[1]].append("%s:%s" % (tup[2], tup[3]))
        else:
            print("struct length not match")
            return None
    for i in range(1000):
        if i not in res_dic:
            break
        schema += res_dic[i]
    return "struct<%s>" % ",".join(schema)


#print(build_nested_schema(v6))
#print(build_flat_schema(v6))
#print build_nested_schema(click_bid)
#print build_flat_schema(click_bid)
#print(build_nested_schema(impression_bid))
print(build_flat_schema(impression_bid))

#print build_nested_schema(report_base)
#print build_nested_schema(report_base_day)
#print build_nested_schema(report_conversion_click)
#print build_nested_schema(report_reach_click)
#print build_nested_schema(report_second_jump)
#print build_nested_schema(report_stats_service)
#print build_nested_schema(report_pdb_analysis)
#print build_nested_schema(report_conversion_imp)
#print build_nested_schema(report_other_cvt)
#print(build_nested_schema(rpt_effect_pdb_return_reason))