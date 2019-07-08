# -*- coding: utf-8 -*-
import sys
import os
import pdb
import json
class CreateParams(object):
    
    def __init__(self, strategy_name):
        self._strategy_name = strategy_name
        self._params_dict = {}
        self.read_rule()
        
    def set_params_scope(self, key, start, end, scope, params_dict):
        params_list = []
        while start <= end:
            if not isinstance(scope, int):
                start = round(start,2)
            params_list.append(start)
            start += scope
        params_dict[key] = params_list
        return params_dict
        
    def set_params_array(self, key, arrays, params_dict):
        params_dict[key] = arrays
        return params_dict
            
    def set_params(self, key, value, params_dict):
        vlist = []
        vlist.append(value)
        params_dict[key] = vlist
        return params_dict
    
    def merge_params(self, params_list, key, params):
        result = []
        if len(params_list) == 0:
            is_new = 1
        else:
            is_new = 0
        for pm in params:
            if is_new:
                result.append({key:pm})
            else:
                 for p in params_list:
                    pt = p.copy()
                    pt[key] = pm
                    result.append(pt)
        return result
                    
    def param_rule(self, job, params_dict):
        jtype = job.get('type')
        jkey = job.get('key')
        if jtype == "scope":
            jstart = job.get('start')
            jend = job.get('end')
            jsection = job.get('section')
            params_dict = self.set_params_scope(jkey, jstart, jend, jsection, params_dict)
        elif jtype == "array":
            jarray = job.get('array')
            params_dict = self.set_params_array(jkey, jarray, params_dict)
        else:
            params_dict = self.set_params(jkey, job.get('value'), params_dict)
        return params_dict
            
    def read_rule(self):
        rule_file = 'rule.json'#str(self._strategy_name) + '/' + 'rule.json'
        with open(rule_file,'rb') as f:
            content = f.read()
            json_ob = json.loads(content)
            for obt in json_ob:
                params_dict = {}
                params = obt['params']
                for ob in params:
                    params_dict = self.param_rule(ob, params_dict)
                self._params_dict[obt['name']] = params_dict
                    
                    
    def create_params(self):
        params_all = {}
        for name, params_dict in self._params_dict.items():
            params_sets = []
            for key in params_dict:
                params_sets = self.merge_params(params_sets, key, params_dict[key])
            params_all[name] = params_sets
            
        all_file  = str(self._strategy_name) + '_' + 'param.json'
        if os.path.exists(str(all_file)):
            os.remove(str(all_file))
        with open(all_file, 'w') as f:
            f.write(json.dumps(params_all)) 
    
    
if __name__ == "__main__":
    pdb.set_trace()
    params = CreateParams('Alpha191')
    params.create_params()
